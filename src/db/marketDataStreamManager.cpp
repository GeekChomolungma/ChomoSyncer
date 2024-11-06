#include "db/marketDataStreamManager.h"

std::string GLOBAL_KLINES_STREAM = "global_klines_stream";
std::string GLOBAL_KLINES_GROUP = "global_klines_group";

// MarketDataStreamManager Constructor
MarketDataStreamManager::MarketDataStreamManager(const std::string& redisHost, int redisPort) : redisHost(redisHost), redisPort(redisPort), redisContextProducer(nullptr), redisContextConsumer(nullptr) {
    connectToRedis();
}

// MarketDataStreamManager Destructor
MarketDataStreamManager::~MarketDataStreamManager() {
    disconnectFromRedis();
}

// Data Publishing Methods
void MarketDataStreamManager::publishGlobalKlines(const std::string& data) {
    if (redisContextProducer) {
        redisReply* reply = (redisReply*)redisCommand(redisContextProducer, "XADD %s * data %s", GLOBAL_KLINES_STREAM.c_str(), data.c_str());
        if (reply == nullptr) {
            std::cerr << "Failed to publish data to stream: " << GLOBAL_KLINES_STREAM << std::endl;
        } else {
            freeReplyObject(reply);
        }
    }
}

void MarketDataStreamManager::publishMarketData(const std::string& asset, const std::string& timeframe, const std::string& data) {
    if (redisContextConsumer) {
        std::string streamName = asset + "-" + timeframe + "-stream";
        redisReply* reply = (redisReply*)redisCommand(redisContextConsumer, "XADD %s * data %s", streamName.c_str(), data.c_str());
        if (reply == nullptr) {
            std::cerr << "Failed to publish data to stream: " << streamName << std::endl;
        } else {
            freeReplyObject(reply);
        }
    }
}

// Data Consumption Methods
std::string MarketDataStreamManager::fetchGlobalKlinesAndDispatch(const std::string& consumerName) {
    // Execute the XGROUP CREATE command
    redisReply* reply = (redisReply*)redisCommand(redisContextConsumer,
        "XGROUP CREATE %s %s $ MKSTREAM", GLOBAL_KLINES_STREAM.c_str(), GLOBAL_KLINES_GROUP.c_str());
    if (reply != nullptr) {
        freeReplyObject(reply);
    }

    reply = (redisReply*)redisCommand(redisContextConsumer, "XREADGROUP GROUP %s %s STREAMS %s >", GLOBAL_KLINES_GROUP.c_str(), consumerName.c_str(), GLOBAL_KLINES_STREAM.c_str());
    if (reply != nullptr && reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {
        redisReply* messages = reply->element[0]->element[1]; // reply->element[0]->element[0] is the stream name
        if (messages->type == REDIS_REPLY_ARRAY && messages->elements > 0) {
            for (size_t i = 0; i < messages->elements; ++i) {
                redisReply* message = messages->element[i];
                std::string messageId = message->element[0]->str;
                std::string messageData = message->element[1]->element[1]->str;
                std::cout << "fetchGlobalKlines data: " << messageData << std::endl;

                // step 0: json parse and ack 0th message
                nlohmann::json j;
                try {
                    j = nlohmann::json::parse(messageData);
                    // first resp maybe like {"result":null,"id":1}
                    auto result = j.find("result");
                    if (result != j.end() && result->is_null()) {
                        continue;
                    }
                    redisCommand(redisContextConsumer, "XACK %s %s %s", GLOBAL_KLINES_STREAM.c_str(), GLOBAL_KLINES_GROUP.c_str(), messageId.c_str());
                } catch (const std::exception& e) {
                    std::cerr << "Error deserializing message: " << e.what() << std::endl;
                    return "";
                }

                // step 1: deserialize from json
                KlineResponse kline; 
                try
                {
                    kline = KlineResponse::deserializeFromJson(j);
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                    return "";
                }  
                
                // step 2: persistence to mongo

                // step 3: publish to asset-timeframe stream
                std::cout << "Publishing to asset-timeframe:" << kline.Symbol << "-" << kline.Interval << "-stream" << std::endl;
                publishMarketData(kline.Symbol, kline.Interval, messageData);
            }
            freeReplyObject(reply);
            return "";
        }
    }
    if (reply) freeReplyObject(reply);
    return "";
}

std::string MarketDataStreamManager::consumeData(const std::string& asset, const std::string& timeframe, const std::string& consumerName) {
    if (redisContextConsumer) {
        // Create the consumer group
        std::string streamName = asset + "-" + timeframe + "-stream";
        std::string groupName = asset + "-group";
        
        // Execute the XGROUP CREATE command
        redisReply* reply = (redisReply*)redisCommand(redisContextConsumer,
            "XGROUP CREATE %s %s $ MKSTREAM", streamName.c_str(), groupName.c_str());
        if (reply != nullptr) {
            freeReplyObject(reply);
        }

        reply = (redisReply*)redisCommand(redisContextConsumer, "XREADGROUP GROUP %s %s STREAMS %s >", groupName.c_str(), consumerName.c_str(), streamName.c_str());
        if (reply != nullptr && reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {
            redisReply* messages = reply->element[0]->element[1];
            if (messages->type == REDIS_REPLY_ARRAY && messages->elements > 0) {
                for (size_t i = 0; i < messages->elements; ++i) {
                    redisReply* message = messages->element[i];
                    std::string messageId = message->element[0]->str;
                    std::string messageData = message->element[1]->element[1]->str;
                }
                freeReplyObject(reply);
                return "";
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
