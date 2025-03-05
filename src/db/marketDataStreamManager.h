#include <hiredis/hiredis.h>
#include "dtos/kline.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <string>

// Example "reply" format from XREADGROUP command:
// for multiple streams:
// [
//     ["stream1", [["1680859830574-0", ["data", "Hello World"]]]],
//     ["stream2", [["1680859830575-0", ["data", "Another Message"]]]]
// ]
// for one stream:
// [
//     ["BTC-1D-stream"(actually the stream1), [ 
//         ["1680859830574-0", ["data", "Hello World"]],
//         ["1680859830575-0", ["data", "Hello Again"]]
//     ]]
// ]

// A Redis wrapper class for managing market data streams
class MarketDataStreamManager {
public:
    // Constructor & Destructor
    MarketDataStreamManager(const std::string& redisHost, int redisPort);
    ~MarketDataStreamManager();

    // Data Publishing Methods
    void publishGlobalKlines(const std::string& data);
    void publishMarketData(const std::string& asset, const std::string& timeframe, const std::string& data);

    // Data Consumption Methods
    std::vector<KlineResponseWs> fetchGlobalKlinesAndDispatch(const std::string& consumerName);
    std::string consumeData(const std::string& asset, const std::string& timeframe, const std::string& consumerName); // todo:: strategy is the consumer for those dispatched data
    
    void acknowledgeMessage(const std::string& asset, const std::string& timeframe, const std::string& messageId);

    // Persistence Methods
    void persistData(); // todo:: persist redis data to db, not just the klines

private:
    // Private Helper Methods
    void connectToRedis();
    void disconnectFromRedis();
    void createConsumerGroup(const std::string& asset, const std::string& timeframe);
    void trimStream(const std::string& asset, const std::string& timeframe);

    // Member Variables
    std::string redisHost;
    int redisPort;
    redisContext* redisContextProducer;
    redisContext* redisContextConsumer;
    std::atomic<bool> keepRunning;
};
