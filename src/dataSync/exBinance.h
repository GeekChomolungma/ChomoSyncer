#include <cstdint>
#include <iomanip>  // for std::put_time
#include <ctime>    // for std::tm, std::localtime
#include <iostream>
#include <memory>
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
#include "ta/indicator_manager.h"
#include "config/config.h"

// when subscribe a multi combined streams in binance，payload will be like {"stream":"<streamName>","data":<rawPayload>}
// but for kline, it is received one by one even in multi streams
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;

using tcp = boost::asio::ip::tcp;
using WsStream = boost::beast::websocket::stream<boost::beast::ssl_stream<boost::asio::ip::tcp::socket>>;

// Class for handling Binance data synchronization
class BinanceDataSync : public std::enable_shared_from_this<BinanceDataSync>{
public:
    BinanceDataSync(const std::string& iniConfig);
    void start();

    // WS Ping/Pong
    void startPing();
    void stopPing();
    void scheduleNextPing();
    
    // Handle history market data synchronization
    void handle_history_market_data_sync();

    // Handle incoming WebSocket messages
    void handle_market_data_subscribe();
    
    // Handle data persistence
    void handle_data_persistence();

private:
    std::string subscribeRequest(const std::vector<std::string>& symbol, const std::vector<std::string>& interval);

    bool connect_noexcept();
    void reset_websocket();

    void asyncReadLoop();

    void scheduleReconnect();

    inline int64_t now_in_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    inline bool is_closed_by_time(const Kline& k, int64_t now_local_ms, int64_t grace_ms = 1000) {
        return now_local_ms >= (k.EndTime + grace_ms);
    }

    // TODO: add the server UTC time interface
    inline int64_t fetch_server_time_ms(const std::string& base_url = "https://api.binance.com", bool spot = true)
    {
        return 0;
    };

    void syncOneSymbol(std::string symbol, std::string interval, uint64_t limit);

    std::vector<KlineResponseWs>  klineRestReq(std::string symbolUpperCase, std::string interval, std::string startTime, std::string endTime, std::string limitStr);

    Config cfg;
    MarketDataStreamManager mkdsM;
    MongoManager mongoM;
    IndicatorManager indicatorM;

    std::vector<std::string> marketSymbols;
    std::vector<std::string> marketIntervals;

    std::chrono::steady_clock::time_point last_persist_time;
    const size_t BATCH_SIZE = 100;
    const std::chrono::seconds BATCH_TIMEOUT = std::chrono::seconds(2);

    // io_context and resolver for asynchronous operations,
    // Could be used in data sync's Multi-Connection-One-Thread model
    net::io_context ioc_;
    net::ssl::context ssl_ctx_;
    net::executor_work_guard<net::io_context::executor_type> work_guard_; // block io_context.run() to keep io_context working all the time

    // WS Ping/Pong
    net::steady_timer ping_timer_;
    bool ping_running_ = false;
    std::chrono::seconds ping_interval_{ std::chrono::minutes(10) };

    // ensure all operations are executed in the same thread
    // Although io_context only run in one thread, we still use strand to ensure all operations are executed in the same order
    // Meanwhile, in the future, when we expand the run threads to multi-threads, strand will ensure the operations are executed in the same order
    net::strand<net::io_context::executor_type> strand_;                  

    tcp::resolver resolver_;
    beast::flat_buffer buffer_;

    std::unique_ptr<WsStream> ws_stream_;
    net::steady_timer reconnect_timer_;
    bool reconnecting_ = false;
};
