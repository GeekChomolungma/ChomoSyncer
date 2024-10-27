#include "db/marketDataStreamManager.h"

// MarketDataStreamManager Constructor
MarketDataStreamManager::MarketDataStreamManager(const std::string& redisHost, int redisPort) : redisHost(redisHost), redisPort(redisPort), redisContextProducer(nullptr), redisContextConsumer(nullptr) {
    connectToRedis();
}

// Data Publishing Methods
void MarketDataStreamManager::publishMarketData(const std::string& asset, const std::string& timeframe, const std::string& data) {
    if (redisContextProducer) {
        std::string streamName = asset + "-" + timeframe + "-stream";
        redisReply* reply = (redisReply*)redisCommand(redisContextProducer, "XADD %s * data %s", streamName.c_str(), data.c_str());
        if (reply == nullptr) {
            std::cerr << "Failed to publish data to stream: " << streamName << std::endl;
        } else {
            freeReplyObject(reply);
        }
    }
}

// Data Consumption Methods
std::string MarketDataStreamManager::consumeData(const std::string& asset, const std::string& timeframe, const std::string& consumerName) {
    if (redisContextConsumer) {
        std::string streamName = asset + "-" + timeframe + "-stream";
        redisReply* reply = (redisReply*)redisCommand(redisContextConsumer, "XREADGROUP GROUP %s %s STREAMS %s >", (asset + "-group").c_str(), consumerName.c_str(), streamName.c_str());
        if (reply != nullptr && reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {
            redisReply* messages = reply->element[0]->element[1];
            if (messages->type == REDIS_REPLY_ARRAY && messages->elements > 0) {
                redisReply* message = messages->element[0];
                std::string messageId = message->element[0]->str;
                std::string messageData = message->element[1]->element[1]->str;
                freeReplyObject(reply);
                return messageData;
            }
        }
        if (reply) freeReplyObject(reply);
    }
    return "";
}

void MarketDataStreamManager::acknowledgeMessage(const std::string& asset, const std::string& timeframe, const std::string& messageId) {
    if (redisContextConsumer) {
        std::string streamName = asset + "-" + timeframe + "-stream";
        redisCommand(redisContextConsumer, "XACK %s %s %s", streamName.c_str(), (asset + "-group").c_str(), messageId.c_str());
    }
}

// Persistence Methods
void MarketDataStreamManager::persistData() {
    // Placeholder: Logic to persist data to MongoDB or other storage.
    std::cout << "Persisting data to MongoDB..." << std::endl;
}

// JSON Serialization/Deserialization
std::string MarketDataStreamManager::serializeToJson(const Kline& kline) {
    nlohmann::json j;
    j["Id"] = kline.Id;
    j["StartTime"] = kline.StartTime;
    j["EndTime"] = kline.EndTime;
    j["Symbol"] = kline.Symbol;
    j["Interval"] = kline.Interval;
    j["FirstTradeID"] = kline.FirstTradeID;
    j["LastTradeID"] = kline.LastTradeID;
    j["Open"] = kline.Open;
    j["Close"] = kline.Close;
    j["High"] = kline.High;
    j["Low"] = kline.Low;
    j["Volume"] = kline.Volume;
    j["TradeNum"] = kline.TradeNum;
    j["IsFinal"] = kline.IsFinal;
    j["QuoteVolume"] = kline.QuoteVolume;
    j["ActiveBuyVolume"] = kline.ActiveBuyVolume;
    j["ActiveBuyQuoteVolume"] = kline.ActiveBuyQuoteVolume;
    j["TrueRange"] = kline.TrueRange;
    j["AveTrueRange"] = kline.AveTrueRange;
    j["SuperTrendValue"] = kline.SuperTrendValue;
    j["StUp"] = kline.StUp;
    j["StDown"] = kline.StDown;
    j["STDirection"] = kline.STDirection;
    j["Action"] = kline.Action;
    return j.dump();
}

Kline MarketDataStreamManager::deserializeFromJson(const std::string& jsonStr) {
    nlohmann::json j = nlohmann::json::parse(jsonStr);
    Kline kline;
    std::strncpy(kline.Id, j["Id"].get<std::string>().c_str(), sizeof(kline.Id));
    kline.StartTime = j["StartTime"].get<int64_t>();
    kline.EndTime = j["EndTime"].get<int64_t>();
    std::strncpy(kline.Symbol, j["Symbol"].get<std::string>().c_str(), sizeof(kline.Symbol));
    std::strncpy(kline.Interval, j["Interval"].get<std::string>().c_str(), sizeof(kline.Interval));
    kline.FirstTradeID = j["FirstTradeID"].get<int64_t>();
    kline.LastTradeID = j["LastTradeID"].get<int64_t>();
    kline.Open = j["Open"].get<double>();
    kline.Close = j["Close"].get<double>();
    kline.High = j["High"].get<double>();
    kline.Low = j["Low"].get<double>();
    kline.Volume = j["Volume"].get<double>();
    kline.TradeNum = j["TradeNum"].get<int64_t>();
    kline.IsFinal = j["IsFinal"].get<bool>();
    kline.QuoteVolume = j["QuoteVolume"].get<double>();
    kline.ActiveBuyVolume = j["ActiveBuyVolume"].get<double>();
    kline.ActiveBuyQuoteVolume = j["ActiveBuyQuoteVolume"].get<double>();
    kline.TrueRange = j["TrueRange"].get<double>();
    kline.AveTrueRange = j["AveTrueRange"].get<double>();
    kline.SuperTrendValue = j["SuperTrendValue"].get<double>();
    kline.StUp = j["StUp"].get<double>();
    kline.StDown = j["StDown"].get<double>();
    kline.STDirection = j["STDirection"].get<int>();
    kline.Action = j["Action"].get<int>();
    return kline;
}

// Private Helper Methods
void MarketDataStreamManager::connectToRedis() {
    redisContextProducer = redisConnect(redisHost.c_str(), redisPort);
    if (redisContextProducer == nullptr || redisContextProducer->err) {
        std::cerr << "Producer connection error: " << (redisContextProducer ? redisContextProducer->errstr : "can't allocate redis context") << std::endl;
    }

    redisContextConsumer = redisConnect(redisHost.c_str(), redisPort);
    if (redisContextConsumer == nullptr || redisContextConsumer->err) {
        std::cerr << "Consumer connection error: " << (redisContextConsumer ? redisContextConsumer->errstr : "can't allocate redis context") << std::endl;
    }
}

void MarketDataStreamManager::disconnectFromRedis() {
    if (redisContextProducer) {
        redisFree(redisContextProducer);
        redisContextProducer = nullptr;
    }
    if (redisContextConsumer) {
        redisFree(redisContextConsumer);
        redisContextConsumer = nullptr;
    }
}

void MarketDataStreamManager::createConsumerGroup(const std::string& asset, const std::string& timeframe) {
    if (redisContextConsumer) {
        std::string streamName = asset + "-" + timeframe + "-stream";
        redisReply* reply = (redisReply*)redisCommand(redisContextConsumer, "XGROUP CREATE %s %s $ MKSTREAM", streamName.c_str(), (asset + "-group").c_str());
        if (reply != nullptr) {
            freeReplyObject(reply);
        }
    }
}

void MarketDataStreamManager::trimStream(const std::string& asset, const std::string& timeframe) {
    if (redisContextProducer) {
        std::string streamName = asset + "-" + timeframe + "-stream";
        redisCommand(redisContextProducer, "XTRIM %s MAXLEN ~ 1000", streamName.c_str());
    }
}
