#include <hiredis/hiredis.h>
#include <nlohmann/json.hpp>
#include "dtos/kline.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <string>

class MarketDataStreamManager {
public:
    // Constructor & Destructor
    MarketDataStreamManager(const std::string& redisHost, int redisPort);
    ~MarketDataStreamManager();

    // Data Publishing Methods
    void publishMarketData(const std::string& asset, const std::string& timeframe, const std::string& data);

    // Data Consumption Methods
    std::string consumeData(const std::string& asset, const std::string& timeframe, const std::string& consumerName);
    void acknowledgeMessage(const std::string& asset, const std::string& timeframe, const std::string& messageId);

    // Persistence Methods
    void persistData(); // Persist data to MongoDB or other storage

    // JSON Serialization/Deserialization
    std::string serializeToJson(const Kline& kline);
    Kline deserializeFromJson(const std::string& jsonStr);

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
