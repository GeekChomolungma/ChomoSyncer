#include "db/marketDataStreamManager.h"
#include <optional>

std::string GLOBAL_KLINES_STREAM = "global_klines_stream";
std::string GLOBAL_KLINES_GROUP = "global_klines_group";

// MarketDataStreamManager Constructor
MarketDataStreamManager::MarketDataStreamManager(const std::string& redisHost, int redisPort, std::string redisPassword) : redisHost(redisHost), redisPort(redisPort), redisContextProducer(nullptr), redisContextConsumer(nullptr) {
    // check the password
    if(!redisPassword.empty()) {
        this->redisPassword = redisPassword;
    }

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

        // trim the symbol stream to keep the latest 10000 messages
        std::cout << "Trimming stream " << streamName << " to keep the latest 10000 messages." << std::endl;
        redisCommand(redisContextConsumer, "XTRIM %s MAXLEN ~ %d", streamName.c_str(), 10000);
    }
}

std::optional<std::pair<std::string, std::string>> parseStreamMessage(redisReply* message) {
    if (message == nullptr || message->type != REDIS_REPLY_ARRAY || message->elements != 2) {
        std::cerr << "Invalid message format: not a 2-element array." << std::endl;
        return std::nullopt;
    }

    // parse messageId
    redisReply* idReply = message->element[0];
    if (idReply->type != REDIS_REPLY_STRING) {
        std::cerr << "Invalid message ID format." << std::endl;
        return std::nullopt;
    }
    std::string messageId = idReply->str;

    // parse field-value pairs
    redisReply* fieldArray = message->element[1];
    if (fieldArray->type != REDIS_REPLY_ARRAY || fieldArray->elements % 2 != 0) {
        std::cerr << "Invalid field-value array." << std::endl;
        return std::nullopt;
    }

    // iterate through field-value pairs
    for (size_t i = 0; i < fieldArray->elements; i += 2) {
        redisReply* field = fieldArray->element[i];
        redisReply* value = fieldArray->element[i + 1];
        if (field->type == REDIS_REPLY_STRING && value->type == REDIS_REPLY_STRING) {
            if (std::string(field->str) == "data") {
                return std::make_pair(messageId, value->str);
            }
        }
    }

    std::cerr << "No 'data' field found in message." << std::endl;
    return std::nullopt;
}

// Data Consumption Methods
std::vector<KlineResponseWs> MarketDataStreamManager::fetchGlobalKlinesAndDispatch(const std::string& consumerName) {
    // Execute the XGROUP CREATE command
    redisReply* reply = (redisReply*)redisCommand(redisContextConsumer,
        "XGROUP CREATE %s %s $ MKSTREAM", GLOBAL_KLINES_STREAM.c_str(), GLOBAL_KLINES_GROUP.c_str());
    if (reply != nullptr) {
        freeReplyObject(reply);
    }

    // new a vector to store the data
    std::vector<KlineResponseWs> finalklines;

    reply = (redisReply*)redisCommand(redisContextConsumer, "XREADGROUP GROUP %s %s STREAMS %s >", GLOBAL_KLINES_GROUP.c_str(), consumerName.c_str(), GLOBAL_KLINES_STREAM.c_str());
    if (reply != nullptr && reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {
        // check how many messages are in the reply    
        std::cout << "Fetched from Redis global_kline_stream " << reply->element[0]->element[1]->elements << " latest market info with different symbols" << std::endl;
        redisReply* messages = reply->element[0]->element[1]; // reply->element[0] is the redis 0th stream if you Xread multi streams, and reply->element[0]->element[0] is the stream name
        if (messages->type == REDIS_REPLY_ARRAY && messages->elements > 0) {
            for (size_t i = 0; i < messages->elements; ++i) {
                redisReply* message = messages->element[i];
                auto parsed = parseStreamMessage(message);
                if (!parsed.has_value()) {
                    continue; // skip this message
                }
                const std::string& messageId = parsed->first;
                const std::string& messageData = parsed->second;

                std::cout << "fetchGlobalKlines data: " << messageData << std::endl;

                // step 0: json parse and ack 0th message
                nlohmann::json j;
                try {
                    j = nlohmann::json::parse(messageData);
                    // first resp maybe like {"result":null,"id":1}
                    auto result = j.find("result");
                    if (result != j.end() && result->is_null()) {
                        redisCommand(redisContextConsumer, "XACK %s %s %s", GLOBAL_KLINES_STREAM.c_str(), GLOBAL_KLINES_GROUP.c_str(), messageId.c_str());
                        continue;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Error deserializing message: " << e.what() << std::endl;
                    return finalklines;
                }

                // step 1: deserialize from json
                KlineResponseWs kline; 
                try
                {
                    kline = KlineResponseWs::deserializeFromJson(j);
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                    return finalklines;
                }  
                
                try
                {
                    // step 2: persistence to mongo
                    if (kline.IsFinal) {
                        // final kline found, put it in the finalklines vector
                        finalklines.push_back(kline);
                    }

                    // step 3: publish to asset-timeframe stream
                    std::cout << "Publishing to asset-timeframe:" << kline.Symbol << "-" << kline.Interval << "-stream" << std::endl;
                    publishMarketData(kline.Symbol, kline.Interval, messageData);
                    
                    // step 4: ack the message
                    // acknowledgeMessage(GLOBAL_KLINES_STREAM, GLOBAL_KLINES_GROUP, messageId);
                    redisCommand(redisContextConsumer, "XACK %s %s %s", GLOBAL_KLINES_STREAM.c_str(), GLOBAL_KLINES_GROUP.c_str(), messageId.c_str());
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << '\n';
                    continue;
                }
            }
            freeReplyObject(reply);
            reply = nullptr;
        }

        // step 5: trim the stream
        std::cout << "Trimming global klines stream to keep the latest 10000 messages." << std::endl;
        redisCommand(redisContextConsumer, "XTRIM %s MAXLEN ~ %d", GLOBAL_KLINES_STREAM.c_str(), 10000);
    }

    if (reply != nullptr) {
        freeReplyObject(reply);
        reply = nullptr;
    }

    return finalklines;
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

    // auth redis
    if (!redisPassword.empty()) {
        redisReply* reply = (redisReply*)redisCommand(redisContextProducer, "AUTH %s", redisPassword.c_str());
        if (reply == nullptr || redisContextProducer->err) {
            std::cerr << "Producer authentication failed: " << (redisContextProducer->errstr ? redisContextProducer->errstr : "Unknown error") << std::endl;
            redisFree(redisContextProducer);
            redisContextProducer = nullptr;
        }
        freeReplyObject(reply);
    }

    redisContextConsumer = redisConnect(redisHost.c_str(), redisPort);
    if (redisContextConsumer == nullptr || redisContextConsumer->err) {
        std::cerr << "Consumer connection error: " << (redisContextConsumer ? redisContextConsumer->errstr : "can't allocate redis context") << std::endl;
    }

    if (!redisPassword.empty()) {
        redisReply* reply = (redisReply*)redisCommand(redisContextConsumer, "AUTH %s", redisPassword.c_str());
        if (reply == nullptr || redisContextConsumer->err) {
            std::cerr << "Consumer authentication failed: " << (redisContextConsumer->errstr ? redisContextConsumer->errstr : "Unknown error") << std::endl;
            redisFree(redisContextConsumer);
            redisContextConsumer = nullptr;
        }
        freeReplyObject(reply);
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
