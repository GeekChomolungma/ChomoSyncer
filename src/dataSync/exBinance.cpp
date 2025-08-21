#include "exBinance.h"

const std::string DB_MARKETINFO = "market_info";
const uint64_t HARDCODE_KLINE_SYNC_START = 1690840800000;  // 2023-08-01 00:00:00 UTC+8

void printHumanReadableTime(int64_t timestamp_ms) {
    // from milliseconds to seconds
    std::time_t time_sec = timestamp_ms / 1000;

    // transform to std::tm structure
    std::tm* tm_ptr = std::gmtime(&time_sec);  // utc
    // std::tm* tm_ptr = std::localtime(&time_sec); // local time

    // print the time in a human-readable format
    std::cout << "Human-readable time: "
        << std::put_time(tm_ptr, "%Y-%m-%d %H:%M:%S") << " UTC" << std::endl;
}

BinanceDataSync::BinanceDataSync(const std::string& iniConfig) : 
    last_persist_time(std::chrono::steady_clock::now()), 
    ioc_(), work_guard_(net::make_work_guard(ioc_)), resolver_(ioc_), ssl_ctx_(net::ssl::context::tlsv12_client),
    reconnect_timer_(ioc_), ping_timer_(ioc_), strand_(ioc_.get_executor()),
    cfg(iniConfig),
    mkdsM(cfg.getRedisHost(), cfg.getRedisPort(), cfg.getRedisPassword()),
    mongoM(cfg.getDatabaseUri()),
    indicatorM(mongoM)
{      
    ws_stream_ = std::make_unique<WsStream>(ioc_, ssl_ctx_);

    auto redisHost = cfg.getRedisHost();
    auto redisPort = cfg.getRedisPort();
    auto mongoUri = cfg.getDatabaseUri();
    marketSymbols = cfg.getMarketSubInfo("marketsub.symbols");
    marketIntervals = cfg.getMarketSubInfo("marketsub.intervals");
}

void BinanceDataSync::start() {
    // setup indicator manager, load indicators from config, and prepare for each symbol and interval
    indicatorM.loadIndicators(marketSymbols, marketIntervals);

    // load the latest indicator states from MongoDB (hot start)
    indicatorM.loadStates(DB_MARKETINFO, marketSymbols, marketIntervals, 20);

    // start threads for history market data sync
    handle_history_market_data_sync();

    // start two threads for market data subscribe and data persistence
    std::cout << "Start two threads for market data subscribe and data persistence." << std::endl;

    // start the io_context in a separate thread
    // in the future, we can expand the threads to multi-threads
    std::thread io_thread([this]() { ioc_.run(); }); 

    // start the market data subscribe and data persistence threads
    std::thread market_data_thread(&BinanceDataSync::handle_market_data_subscribe, this); // not necessary use thread, but leave thread + io_context post(async) for future expansion
    std::thread data_persistence_thread(&BinanceDataSync::handle_data_persistence, this);
    
    market_data_thread.join();
    data_persistence_thread.join();

    // stop the io_context when all threads are done
    // otherwise, if no ioc stop, because of work_guard_, this join is not reachable, will block.
    ioc_.stop(); 
    io_thread.join(); 
}

void BinanceDataSync::handle_market_data_subscribe() {
    try {
        net::post(strand_, [this, self = shared_from_this()] {
            if (!connect_noexcept()) { scheduleReconnect(); return; }
            startPing(); // start the ping timer to keep the connection alive
            asyncReadLoop(); // start the async read loop to receive messages
            });
    } catch (const std::exception &e) {
        std::cerr << "handle_market_data websocket error: " << e.what() << std::endl;
    }

    std::cout << "handle_market_data_subscribe Posted" << std::endl;
}
    
void BinanceDataSync::handle_data_persistence() {
    while (true) {
        // fetch global klines and dispatch
        std::vector<KlineResponseWs> closedKlines = mkdsM.fetchGlobalKlinesAndDispatch("consumer1");
        if (closedKlines.empty()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }else{
            std::cout << "Fetched " << closedKlines.size() << " closed klines." << std::endl;
        }

        // step 1: key sorted by symbol and interval
        std::unordered_map<std::string, std::vector<KlineResponseWs>> klinesBySymbolInterval;
        klinesBySymbolInterval.reserve(256); // reserve some space to avoid rehashing
        for (auto& k : closedKlines) {
            std::string key = k.Symbol + "_" + k.Interval;
            klinesBySymbolInterval[key].push_back(k);
        }

        // step 2: update indicators for each symbol and interval
        for (auto& [key, vec] : klinesBySymbolInterval) {
            std::sort(vec.begin(), vec.end(),
                [](auto& a, auto& b) { return a.StartTime < b.StartTime; });

            // step 3: process each kline and update indicators
            for (auto& ws : vec) {
                Kline k = KlineResponseWs::toKline(ws);
                indicatorM.processNewKline(k);
            }

            // step 4: write closed klines to MongoDB
            mongoM.WriteClosedKlines(DB_MARKETINFO, vec);
        }
    }
}

void BinanceDataSync::handle_history_market_data_sync() {
    // sync all symbols and intervals with multi-thread
    try {
        std::vector<std::thread> syncThreads;
        for (auto symbol : marketSymbols) {
            for (auto interval : marketIntervals) {
                syncThreads.push_back(std::thread(&BinanceDataSync::syncOneSymbol, this, symbol, interval, 1000));
            }
        }

        for (auto& t : syncThreads) {
            t.join();
        }
        std::cout << "handle_history_market_data_sync done, go ahead for subscribe market info." << std::endl;
    }
    catch(const std::exception& e) {
        std::cerr << "handle_history_market_data_sync error: " << e.what() << std::endl;
    }
}

void BinanceDataSync::handle_history_gap_fill() {
    std::cout << "Start gap fill" << std::endl;
    handle_history_market_data_sync();
}

std::string BinanceDataSync::subscribeRequest(const std::vector<std::string>& symbol, const std::vector<std::string>& interval) {
    std::ostringstream oss;
    std::string subscribePrefix = "{\"method\": \"SUBSCRIBE\", \"params\": [";
    std::string timeframeSuffix = "kline_";
    
    oss << subscribePrefix;
    for (size_t i = 0; i < symbol.size(); ++i) {
        for (size_t j = 0; j < interval.size(); ++j) {
            oss << "\"" << symbol[i] << "@" << timeframeSuffix << interval[j] << "\",";
        }
    }

    // remove the last comma
    std::string subscribeRequest = oss.str();
    subscribeRequest.pop_back();
    oss.str("");
    oss.clear();
    oss << subscribeRequest;
    oss << "], \"id\": 1}";
    return oss.str();
}

bool BinanceDataSync::connect_noexcept(){
    // Reset the WebSocket stream before connecting
    reset_websocket();
    beast::error_code ec;

    // SNI: set the server name indication for the TLS(SSL) context in Hello message, so that the server can present the correct certificate
    if (!SSL_set_tlsext_host_name(ws_stream_->next_layer().native_handle(), "stream.binance.com")) {
        return false;
    }

    // Resolve the Binance WebSocket server address
    tcp::resolver resolver{ ioc_ };
    auto res = resolver.resolve("stream.binance.com", "9443", ec);
    if (ec) {
        std::cerr << "Error resolving WebSocket server: " << ec.message() << std::endl;
        return false;
    }

    // Connect to the server use TCP layers
    net::connect(ws_stream_->next_layer().next_layer(), res, ec);
    if (ec) {
        std::cerr << "Error connecting to WebSocket server: " << ec.message() << std::endl;
        return false;
    }

    // Perform the TLS handshake(based on the old-school SSL context)
    ws_stream_->next_layer().handshake(net::ssl::stream_base::client, ec);
    if (ec) {
        std::cerr << "Error during TLS handshake: " << ec.message() << std::endl;
        return false;
    }

    // TODO: check the server certificate, if needed
    //SSL_set1_host(ssl, "stream.binance.com");
    //ssl_ctx_.set_verify_mode(boost::asio::ssl::verify_peer);
    //ssl_ctx_.set_default_verify_paths();

    ws_stream_->set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

    // Perform the WebSocket handshake
    ws_stream_->handshake("stream.binance.com", "/ws", ec);
    if (ec) {
        std::cerr << "Error during WebSocket handshake: " << ec.message() << std::endl;
        return false;
    }

    // Send a subscription message to the WebSocket server
    std::string json_message = subscribeRequest(marketSymbols, marketIntervals);
    std::cout << "Sending message: " << json_message << std::endl;
    ws_stream_->write(net::buffer(json_message), ec);
    if (ec) {
        std::cerr << "Error sending subscription message: " << ec.message() << std::endl;
        return false;
    }

    return true;
}

void BinanceDataSync::reset_websocket() {
    beast::error_code ec;
    if (ws_stream_) {
        if (ws_stream_->is_open()) {
            ws_stream_->close(websocket::close_code::normal, ec);
            ec = {};
        }

        auto& tls_session = ws_stream_->next_layer();
        auto& tcp_socket = ws_stream_->next_layer().next_layer();
        
        if (tcp_socket.is_open()) {
            // close the underlying TLS stream
            tls_session.shutdown(ec);
            if (ec) {
                std::cerr << "Error shutting down TLS stream: " << ec.message() << std::endl;
            }

            tcp_socket.shutdown(tcp::socket::shutdown_both, ec);
            if (ec) {
                std::cerr << "Error shutting down TCP socket: " << ec.message() << std::endl;
            }

            tcp_socket.close(ec);
            if (ec) {
                std::cerr << "Error closing TCP socket: " << ec.message() << std::endl;
            }
        }
    }

    // Reset the WebSocket stream
    ws_stream_.reset(new WsStream(ioc_, ssl_ctx_));
    std::cout << "WebSocket stream reset successfully. Ready to re-connect to server..." << std::endl;
}

void BinanceDataSync::asyncReadLoop(){
    auto self = shared_from_this();
    ws_stream_->async_read(
        buffer_,
        net::bind_executor(strand_,
            [this, self](beast::error_code ec, std::size_t n) {  // self is used to keep the shared_ptr alive, add one more reference. otherwise, the shared_ptr may be destroyed before the callback is called.
                if (ec) {
                    // 1. scheduledReconnect has cancelled this operation, taken over by it so no need to reconnect
                    if (ec == net::error::operation_aborted) {
                        std::cerr << "WS read ended: " << ec.message() << std::endl;
                        return;
                    }

                    // 2. if the connection is closed by other reason, then reconnect
                    if (!reconnecting_) {
                        scheduleReconnect();
                    }
                    return; 
                }

                std::string msg = beast::buffers_to_string(buffer_.data());
                
                // buffer_.consume(n);
                buffer_.consume(buffer_.size());

                // std::cout << "AsyncReadLoop received message: " << msg << std::endl;

                // Process the received market data
                mkdsM.publishGlobalKlines(msg);

                // read from the stream
                asyncReadLoop();
            }
        )
    );
}

// TODO:
// 1. Silent packet loss: when DROP packets, the server will not send any error message, so we need to handle this case. 
//    Use watchdog to detect the connection status?
// 2. Important! History data sync: 
//    when the connection is lost for a long time, we need to re-sync the history data for all symbols and intervals

void BinanceDataSync::scheduleReconnect() {
    net::post(strand_, [this, self = shared_from_this()] {
        if (reconnecting_) return;
        reconnecting_ = true;

        // 1. stop ping timer
        ping_running_ = false;
        beast::error_code ec;
        ping_timer_.cancel(ec);

        // 2. close ws and cancel all the suspend I/O, an operation_aborted code will be returned
        if (ws_stream_ && ws_stream_->is_open()) {
            ws_stream_->async_close(websocket::close_code::normal,
                net::bind_executor(strand_, [](beast::error_code) { /* quiet */ }));
        }

        // 3. stop reconnect timer
        ec = {};
        reconnect_timer_.cancel(ec);
        reconnect_timer_.expires_after(std::chrono::seconds(5));
        reconnect_timer_.async_wait(
            net::bind_executor(strand_,  // also put the reconnect operation in the strand
                [this, self](beast::error_code tec) {
                    reconnecting_ = false;
                    if (tec == net::error::operation_aborted) return;
                    if (!connect_noexcept()) { scheduleReconnect(); return; }
                    startPing(); // restart the ping timer to keep the connection alive
                    asyncReadLoop();

                    // history Klines gap fill
                    std::thread([this] {
                        // atomic flag
                        if (gapfill_running_.exchange(true)) return;
                        handle_history_gap_fill();
                        gapfill_running_ = false;
                    }).detach();

                }
            )
        );
    });
}

void BinanceDataSync::syncOneSymbol(std::string symbol, std::string interval, uint64_t limit){
    // convert limit to string
    std::ostringstream oss;
    oss << limit;
    std::string limitStr = oss.str();
    std::string upperCaseSymbol(symbol);
    std::transform(upperCaseSymbol.begin(), upperCaseSymbol.end(), upperCaseSymbol.begin(), ::toupper);
       
    //// Prepare the indicator manager for this symbol and interval
    //indicatorM.prepare(DB_MARKETINFO, upperCaseSymbol, interval, 20); // pre-retrieve history klines for indicators

    while(true){
        // check start time from mongo lasted kline
        int64_t startTime = 0;
        int64_t endTime = 0;
        mongoM.GetLatestSyncedTime(DB_MARKETINFO, upperCaseSymbol + "_" + interval + "_Binance", startTime, endTime);
        int64_t nextStartMs = (startTime == 0) ? HARDCODE_KLINE_SYNC_START : (endTime + 1);

        // from milliseconds to seconds
        std::time_t time_sec = nextStartMs / 1000;
        // transform to std::tm structure
        std::tm* tm_ptr = std::gmtime(&time_sec);  // utc

        // print the time in a human-readable format
        std::cout << "SyncOneSymbol starts from the next starttime time: ";
        std::cout << std::put_time(tm_ptr, "%Y-%m-%d %H:%M:%S") << " UTC: " << nextStartMs << " for " << upperCaseSymbol << "_" << interval << std::endl;

        // int_64 endTime convert to string
        std::string nextStartTime = std::to_string(nextStartMs);
        auto FetchedKlines_ws = klineRestReq(upperCaseSymbol, interval, nextStartTime, "", limitStr);

        // a tmp vector for those to be written to mongo
        std::vector<KlineResponseWs> KlinesToBeWritten_ws;
        KlinesToBeWritten_ws.reserve(FetchedKlines_ws.size());

        // filter the Fetched klines, remove the latest one as it's not closed yet.
        for (auto& kline_ws : FetchedKlines_ws) {
            // convert KlineResponseWs to Kline
            Kline klineInst = KlineResponseWs::toKline(kline_ws);

            bool closed = is_closed_by_time(klineInst, now_in_ms());
            if (!closed) {
                std::cout << "Processing non-final kline: " << klineInst.StartTime << " for " << upperCaseSymbol << "_" << interval << std::endl;
                continue;
            }
            
            indicatorM.processNewKline(klineInst);
            KlinesToBeWritten_ws.push_back(kline_ws); // make sure non-final out
        }

        if (KlinesToBeWritten_ws.empty()) {
            std::cout << "syncOneSymbol reach end, No closed klines to write for " << upperCaseSymbol << "_" << interval << std::endl;
            return; // no closed klines to write, exit the loop
        }

        // Write the klines to MongoDB
        auto colName = upperCaseSymbol + "_" + interval + "_Binance";
        mongoM.BulkWriteClosedKlines(DB_MARKETINFO, colName, KlinesToBeWritten_ws);

        if (KlinesToBeWritten_ws.size() < limit) {
            // means the data is up to date
            std::cout << "syncOneSymbol reach end, Fetched last " << KlinesToBeWritten_ws.size() << " klines for " << upperCaseSymbol << "_" << interval << std::endl;
            return;
        }
    }
}

std::vector<KlineResponseWs> BinanceDataSync::klineRestReq(std::string symbolUpperCase, std::string interval, std::string startTime, std::string endTime, std::string limitStr) {
    // Important! Create a new io_context and SSL context for HTTP request
    net::io_context http_ioc;
    net::ssl::context http_ssl(net::ssl::context::tlsv12_client);

    // Set up HTTP request
    beast::ssl_stream<tcp::socket> stream(http_ioc, http_ssl);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), "api.binance.com")) {
        beast::error_code ec{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
        throw beast::system_error{ec};
    }

    // New TCP resolver for the HTTP request
    tcp::resolver resolver(http_ioc);
    auto results = resolver.resolve("api.binance.com", "https");
    net::connect(stream.next_layer(), results.begin(), results.end());
    stream.handshake(net::ssl::stream_base::client);

    // Set up the target URL and HTTP request parameters
    std::string target = "/api/v3/klines?symbol=" + symbolUpperCase + "&interval=" + interval + "&startTime=" + startTime + "&limit=" + limitStr;
    std::cout << "Requesting: " << target << std::endl;
    int version = 11;

    http::request<http::string_body> req{http::verb::get, target, version};
    req.set(http::field::host, "api.binance.com");
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::content_type, "application/json");
    // req.set("X-MBX-APIKEY", "YOUR_API_KEY");

    // Send the HTTP request to the remote host
    http::write(stream, req);

    // Buffer for reading the response
    beast::flat_buffer buffer;

    // Container for the response
    http::response<http::dynamic_body> res;

    // Receive the HTTP response
    http::read(stream, buffer, res);

    // Gracefully close the stream
    beast::error_code ec;
    stream.shutdown(ec);
    if (ec == net::error::eof) {
        ec = {};
    }

    std::vector<KlineResponseWs> wsKlines;
    // Parse the response and insert into MongoDB
    if (res.result() == http::status::ok) {
        auto body = boost::beast::buffers_to_string(res.body().data());

        // Parse the kline data and insert into MongoDB
        KlineResponseWs::parseKlineWs(body, symbolUpperCase, interval, wsKlines);
        std::cout << "Fetched " << wsKlines.size() << " klines for " << symbolUpperCase << "_" << interval << std::endl;
        return wsKlines;
    } else {
        std::cerr << "Failed to fetch klines. Status code: " << res.result_int() << std::endl;
        return std::vector<KlineResponseWs>();
    }
}

void BinanceDataSync::startPing() {
    // put the ping operation in the strand to ensure it runs in the correct order
    net::post(strand_, [this, self = shared_from_this()] {
        if (ping_running_) return;
        ping_running_ = true;
        scheduleNextPing();
        });
}

void BinanceDataSync::stopPing() {
    net::post(strand_, [this, self = shared_from_this()] {
        ping_running_ = false;
        beast::error_code ec;
        ping_timer_.cancel(ec); // stop the ping timer
        });
}

// heart beat for the WebSocket connection
void BinanceDataSync::scheduleNextPing() {
    if (!ping_running_ || reconnecting_) return;          // reconnecting or ping not used, then do not schedule next ping
    if (!ws_stream_ || !ws_stream_->is_open()) return;    // no ws or ws not open, then do not schedule next ping

    ping_timer_.expires_after(ping_interval_);
    ping_timer_.async_wait(
        net::bind_executor(strand_,
            [this, self = shared_from_this()](beast::error_code ec) {
                if (!ping_running_) return;
                if (ec == net::error::operation_aborted) return;
                if (!ws_stream_ || !ws_stream_->is_open()) return;

                ws_stream_->async_ping(websocket::ping_data{},
                    net::bind_executor(strand_,
                        [this, self](beast::error_code pec) {
                            if (!ping_running_) return;
                            if (pec == net::error::operation_aborted || pec == websocket::error::closed) return;
                            if (pec) {
                                // reconnect if ping failed
                                scheduleReconnect();
                                return;
                            }
                            scheduleNextPing();
                        })
                );
            })
    );
}