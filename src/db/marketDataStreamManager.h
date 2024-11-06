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
//     ["BTC-1D-stream", [ 
//         ["1680859830574-0", ["data", "Hello World"]],
//         ["1680859830575-0", ["data", "Hello Again"]]
//     ]]
// ]

class MarketDataStreamManager {
public:
    // Constructor & Destructor
    MarketDataStreamManager(const std::string& redisHost, int redisPort);
    ~MarketDataStreamManager();

    // Data Publishing Methods
    void publishGlobalKlines(const std::string& data);
    void publishMarketData(const std::string& asset, const std::string& timeframe, const std::string& data);

    // Data Consumption Methods
    std::string fetchGlobalKlinesAndDispatch(const std::string& consumerName);
    std::string consumeData(const std::string& asset, const std::string& timeframe, const std::string& consumerName);
    
    void acknowledgeMessage(const std::string& asset, const std::string& timeframe, const std::string& messageId);

    // Persistence Methods
    void persistData(); // Persist data to MongoDB or other storage

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
