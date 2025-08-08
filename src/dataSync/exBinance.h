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
#include "ta/indicator_manager.h"
#include "config/config.h"

// when subscribe a multi combined streams in binance，payload will be like {"stream":"<streamName>","data":<rawPayload>}
// but for kline, it is received one by one even in multi streams
using tcp = boost::asio::ip::tcp;
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;

// Class for handling Binance data synchronization
class BinanceDataSync : public std::enable_shared_from_this<BinanceDataSync>{
public:
    BinanceDataSync(const std::string& iniConfig);

    void start();

    void handle_market_data_subscribe();
        
    void handle_data_persistence();
    
    void handle_history_market_data_sync();

private:
    std::string subscribeRequest(const std::vector<std::string>& symbol, const std::vector<std::string>& interval);

    void connect();

    void asyncReadLoop();

    void sendPong(const std::string& ping_message);

    void scheduleReconnect();

    void syncOneSymbol(std::string symbol, std::string interval, u_int64 limit);

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
    net::executor_work_guard<net::io_context::executor_type> work_guard_; // keep io_context running
    tcp::resolver resolver_;
    net::ssl::context ssl_ctx_;
    beast::flat_buffer buffer_;
    websocket::stream<beast::ssl_stream<tcp::socket>> ws_stream_;
    net::steady_timer reconnect_timer_;
};
