#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>

#include "db/marketDataStreamManager.h"
#include "db/mongoManager.h"
#include "config/config.h"

// when subscribe a multi combined streams in binance，payload will be like {"stream":"<streamName>","data":<rawPayload>}
// but for kline, it is received one by one even in multi streams

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;

// Class for handling Binance data synchronization
class BinanceDataSync : public std::enable_shared_from_this<BinanceDataSync>{
public:
    BinanceDataSync(const std::string& iniConfig) : 
        last_persist_time(std::chrono::steady_clock::now()), 
        ioc_(), work_guard_(net::make_work_guard(ioc_)), resolver_(ioc_), ssl_ctx_(net::ssl::context::tlsv12_client),
        ws_stream_(ioc_, ssl_ctx_), reconnect_timer_(ioc_),
        cfg(iniConfig),
        mkdsM(cfg.getRedisHost(), cfg.getRedisPort()),
        mongoM(cfg.getDatabaseUri())
    {
            auto redisHost = cfg.getRedisHost();
            auto redisPort = cfg.getRedisPort();
            auto mongoUri = cfg.getDatabaseUri();
            marketSymbols = cfg.getMarketSubInfo("marketsub.symbols");
            marketIntervals = cfg.getMarketSubInfo("marketsub.intervals");
    }

    void start() {
        std::thread market_data_thread(&BinanceDataSync::handle_market_data, this);
        std::thread data_persistence_thread(&BinanceDataSync::handle_data_persistence, this);

        market_data_thread.join();
        data_persistence_thread.join();
    }

    void handle_market_data() {
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
    
    void handle_data_persistence() {
        while (true) {
            // fecth global klines and dispatch
            std::vector<KlineResponseWs> closedKlines = mkdsM.fetchGlobalKlinesAndDispatch("consumer1");
            if (closedKlines.empty()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }else{
                std::cout << "Fetched " << closedKlines.size() << " closed klines." << std::endl;
            }

            // Write the closed klines to MongoDB
            mongoM.WriteClosedKlines(closedKlines);

            // std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    
    void handle_rest_operations() {
        while (true) {
            std::unique_lock<std::mutex> lock(signal_mtx);
            // Wait for a trade signal from the strategy evaluation thread
            signal_cv.wait(lock, [this] { return !trade_signals.empty(); });

            // Process trade signal
            std::string signal = trade_signals.front();
            trade_signals.pop();
            lock.unlock();  // Unlock mutex before making REST API call

            try {
                // Set up HTTP request
                beast::ssl_stream<tcp::socket> stream(ioc_, ssl_ctx_);
                if(!SSL_set_tlsext_host_name(stream.native_handle(), "api.binance.com")) {
                    beast::error_code ec{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
                    throw beast::system_error{ec};
                }

                auto const results = resolver_.resolve("api.binance.com", "https");
                net::connect(stream.next_layer(), results.begin(), results.end());
                stream.handshake(net::ssl::stream_base::client);

                // Set up the target URL and HTTP request parameters
                std::string target = "/api/v3/order";
                int version = 11;

                http::request<http::string_body> req{http::verb::post, target, version};
                req.set(http::field::host, "api.binance.com");
                req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
                req.set("X-MBX-APIKEY", "YOUR_API_KEY");
                req.body() = "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.001&price=45000";
                req.prepare_payload();

                // Send the HTTP request to the remote host
                http::write(stream, req);

                // Buffer for reading the response
                beast::flat_buffer buffer;

                // Container for the response
                http::response<http::dynamic_body> res;

                // Receive the HTTP response
                http::read(stream, buffer, res);

                // Output the response
                if (res.result() == http::status::ok) {
                    std::cout << "Order successfully placed." << std::endl;
                } else {
                    std::cout << "Failed to place order. Status code: " << res.result_int() << std::endl;
                }

                // Gracefully close the stream
                beast::error_code ec;
                stream.shutdown(ec);
                if (ec == net::error::eof) {
                    ec = {};
                }
                if (ec) {
                    throw beast::system_error{ec};
                }

            } catch (const std::exception &e) {
                std::cerr << "REST API error: " << e.what() << std::endl;
            }
        }
    }

private:
    std::string subscribeRequest(const std::vector<std::string>& symbol, const std::vector<std::string>& interval) {
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

    void connect(){
        try {
            // Resolve the Binance WebSocket server address
            auto const results = resolver_.resolve("stream.binance.com", "9443");

            // Connect to the server
            boost::asio::connect(ws_stream_.next_layer().next_layer(), results.begin(), results.end());

            // Perform the SSL handshake
            ws_stream_.next_layer().handshake(boost::asio::ssl::stream_base::client);

            // Perform the WebSocket handshake
            ws_stream_.handshake("stream.binance.com", "/ws");

            // Send a subscription message to the WebSocket server
            std::string json_message = subscribeRequest(marketSymbols, marketIntervals);
            std::cout << "Sending message: " << json_message << std::endl;
            ws_stream_.write(boost::asio::buffer(json_message)); 

        } catch (const std::exception &e) {
            std::cerr << "connect error: " << e.what() << std::endl;
            // if failed, try to reconnect
            scheduleReconnect();
        }
    }

    void asyncReadLoop(){
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

    void sendPong(const std::string& ping_message) {
        auto self = shared_from_this();
        websocket::ping_data pong_data(ping_message);
        ws_stream_.async_pong(pong_data, [this, self](beast::error_code ec) {
            if (ec) {
                std::cerr << "Pong error: " << ec.message() << std::endl;
                scheduleReconnect();
            }
        });
    }

    void scheduleReconnect(){
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

    Config cfg;
    MarketDataStreamManager mkdsM;
    MongoManager mongoM;
    std::vector<std::string> marketSymbols;
    std::vector<std::string> marketIntervals;

    std::chrono::steady_clock::time_point last_persist_time;
    const size_t BATCH_SIZE = 100;
    const std::chrono::seconds BATCH_TIMEOUT = std::chrono::seconds(2);

    // io_context and resolver for asynchronous operations,
    // Could be used in data sync's Multi-Connection-One-Thread model
    net::io_context ioc_;
    net::executor_work_guard<net::io_context::executor_type> work_guard_; // keep io_context running
    tcp::resolver resolver_;
    net::ssl::context ssl_ctx_;
    beast::flat_buffer buffer_;
    websocket::stream<beast::ssl_stream<tcp::socket>> ws_stream_;
    net::steady_timer reconnect_timer_;

    // rest operations
    std::mutex signal_mtx;
    std::condition_variable signal_cv;
    std::queue<std::string> trade_signals;
};
