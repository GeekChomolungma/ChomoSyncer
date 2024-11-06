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

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;

// Class for handling Binance data synchronization
class BinanceDataSync {
public:
    BinanceDataSync(const std::string& redisHost, int redisPort) : last_persist_time(std::chrono::steady_clock::now()), ioc_(), resolver_(ioc_), ssl_ctx_(net::ssl::context::tlsv12_client), mkdsM(redisHost, redisPort){}

    void start() {
        std::thread market_data_thread(&BinanceDataSync::handle_market_data, this);
        std::thread data_persistence_thread(&BinanceDataSync::handle_data_persistence, this);

        market_data_thread.join();
        data_persistence_thread.join();
    }

    void handle_market_data() {
        try {
            // Resolve the Binance WebSocket server address
            auto const results = resolver_.resolve("stream.binance.com", "9443");

            // Create and open a WebSocket stream
            websocket::stream<beast::ssl_stream<tcp::socket>> ws_stream(ioc_, ssl_ctx_);
            if (!SSL_set_tlsext_host_name(ws_stream.next_layer().native_handle(), "stream.binance.com")) {
                throw beast::system_error(
                    beast::error_code(static_cast<int>(::ERR_get_error()), boost::asio::error::get_ssl_category()));
            }

            // Connect to the server
            boost::asio::connect(ws_stream.next_layer().next_layer(), results.begin(), results.end());

            // Perform the SSL handshake
            ws_stream.next_layer().handshake(boost::asio::ssl::stream_base::client);

            // Perform the WebSocket handshake
            ws_stream.handshake("stream.binance.com", "/ws");

            // Send a subscription message to the WebSocket server
            std::ostringstream oss;
            std::string symbol = "btcusdt";
            std::string timeframe = "kline_15m";
            oss << "{\"method\": \"SUBSCRIBE\", \"params\": [\"" << symbol << "@" << timeframe << "\"], \"id\": 1}";
            std::string json_message = oss.str();
            std::cout << "Sending message: " << json_message << std::endl;
            ws_stream.write(boost::asio::buffer(json_message));

            // Read messages in a loop
            while (true) {
                beast::flat_buffer buffer;
                ws_stream.read(buffer);
                std::string message = beast::buffers_to_string(buffer.data());

                // Process the received market data
                this->mkdsM.publishGlobalKlines(message);
            }

        } catch (const std::exception &e) {
            std::cerr << "WebSocket error: " << e.what() << std::endl;
        }
    }
    

    void handle_data_persistence() {
        while (true) {
            this->mkdsM.fetchGlobalKlinesAndDispatch("consumer1");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
    MarketDataStreamManager mkdsM;
    //MongoManager mongoM;

    std::chrono::steady_clock::time_point last_persist_time;
    const size_t BATCH_SIZE = 100;
    const std::chrono::seconds BATCH_TIMEOUT = std::chrono::seconds(2);
    net::io_context ioc_;
    tcp::resolver resolver_;
    net::ssl::context ssl_ctx_;

    // rest operations
    std::mutex signal_mtx;
    std::condition_variable signal_cv;
    std::queue<std::string> trade_signals;
};
