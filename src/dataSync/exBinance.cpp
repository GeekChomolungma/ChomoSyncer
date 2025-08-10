#include "exBinance.h"

const std::string DB_MARKETINFO = "market_info";
const uint64_t HARDCODE_KLINE_SYNC_START = 1753999200000;  // 2023-01-01 00:00:00 UTC+8

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
    ws_stream_(ioc_, ssl_ctx_), reconnect_timer_(ioc_),
    cfg(iniConfig),
    mkdsM(cfg.getRedisHost(), cfg.getRedisPort(), cfg.getRedisPassword()),
    mongoM(cfg.getDatabaseUri()),
    indicatorM(mongoM)
{
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
    std::thread market_data_thread(&BinanceDataSync::handle_market_data_subscribe, this);
    std::thread data_persistence_thread(&BinanceDataSync::handle_data_persistence, this);
    market_data_thread.join();
    data_persistence_thread.join();
}

void BinanceDataSync::handle_market_data_subscribe() {
    std::thread io_thread([this]() {
        ioc_.run();
    });

    try {
        connect();
        asyncReadLoop();
    } catch (const std::exception &e) {
        std::cerr << "handle_market_data websocket error: " << e.what() << std::endl;
    }

    io_thread.join();
    std::cout << "handle_market_data thread exit." << std::endl;
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

void BinanceDataSync::connect(){
    try {
        // Resolve the Binance WebSocket server address
        auto const results = resolver_.resolve("stream.binance.com", "9443");

        // Connect to the server
        net::connect(ws_stream_.next_layer().next_layer(), results.begin(), results.end());

        // Perform the SSL handshake
        ws_stream_.next_layer().handshake(net::ssl::stream_base::client);

        // Perform the WebSocket handshake
        ws_stream_.handshake("stream.binance.com", "/ws");

        // Send a subscription message to the WebSocket server
        std::string json_message = subscribeRequest(marketSymbols, marketIntervals);
        std::cout << "Sending message: " << json_message << std::endl;
        ws_stream_.write(net::buffer(json_message)); 

    } catch (const std::exception &e) {
        std::cerr << "connect error: " << e.what() << std::endl;
        // if failed, try to reconnect
        scheduleReconnect();
    }
}

void BinanceDataSync::asyncReadLoop(){
    auto self = shared_from_this();
    ws_stream_.async_read(buffer_, [this, self](const boost::system::error_code& ec, std::size_t bytes_transferred){
        if(ec){
            std::cerr << "asyncReadLoop read error: " << ec.message() << std::endl;
            // if failed, try to reconnect
            scheduleReconnect();
            return;
        }

        std::string message = beast::buffers_to_string(buffer_.data());
        buffer_.consume(bytes_transferred);

        std::cout << "asyncReadLoop received message: " << message << std::endl;

        if(message.find("ping") != std::string::npos){
            // Respond to the ping message
            std::cout << "Received ping message: " << message << std::endl;
            sendPong(message);
        }else{
            // Process the received market data
            mkdsM.publishGlobalKlines(message);
        }

        // read from the stream
        asyncReadLoop();
    });
}

void BinanceDataSync::sendPong(const std::string& ping_message) {
    auto self = shared_from_this();
    websocket::ping_data pong_data(ping_message);
    ws_stream_.async_pong(pong_data, [this, self](beast::error_code ec) {
        if (ec) {
            std::cerr << "Pong error: " << ec.message() << std::endl;
            scheduleReconnect();
        }
    });
}

void BinanceDataSync::scheduleReconnect(){
    std::cerr << "Reconnecting to the WebSocket server in 5 seconds..." << std::endl;
    auto self = shared_from_this();
    reconnect_timer_.expires_after(std::chrono::seconds(5));
    reconnect_timer_.async_wait([this, self](const boost::system::error_code& ec){
        if(!ec){
            connect();
            asyncReadLoop();
            return;
        }else{
            std::cerr << "scheduleReconnect reconnect timer error: " << ec.message() << std::endl;
        }
    });
}

void BinanceDataSync::syncOneSymbol(std::string symbol, std::string interval, u_int64 limit){
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
        std::time_t time_sec = startTime / 1000;
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
    // Set up HTTP request
    beast::ssl_stream<tcp::socket> stream(ioc_, ssl_ctx_);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), "api.binance.com")) {
        beast::error_code ec{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
        throw beast::system_error{ec};
    }

    auto const results = resolver_.resolve("api.binance.com", "https");
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
