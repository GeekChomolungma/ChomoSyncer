#ifndef KLINE_H
#define KLINE_H

#include <cstdint>
#include <nlohmann/json.hpp>
#include <iostream>

class Kline {
public:
    // kline info
    char Id[12];
    int64_t StartTime;
    int64_t EndTime;
    char Symbol[16];
    char Interval[16];
    int64_t FirstTradeID;
    int64_t LastTradeID;
    double  Open;
    double  Close;
    double  High;
    double  Low;
    double  Volume;
    int64_t  TradeNum;
    bool IsFinal;
    double  QuoteVolume;
    double  ActiveBuyVolume;
    double  ActiveBuyQuoteVolume;
    double  TrueRange;

    // SuperTrend Indicator
    // need to calculate if length is change
    double  AveTrueRange;
    double  SuperTrendValue;
    double  StUp;
    double  StDown;
    int     STDirection; // -1 1
    int     Action; // 0 is none, 1 is sell, 2 is buy
};

class KlineResponseWs;

class KlineResponseRest {
public:
    uint64_t OpenTime;                   // "openTime": 1672515780000 - Start time of this Kline
    std::string Open;                    // "open": "0.0010" - Opening price
    std::string High;                    // "high": "0.0025" - Highest price during this Kline
    std::string Low;                     // "low": "0.0015" - Lowest price during this Kline
    std::string Close;                   // "close": "0.0020" - Closing price
    std::string Volume;                  // "volume": "1000" - Volume during this Kline
    uint64_t CloseTime;                  // "closeTime": 1672515839999 - End time of this Kline
    std::string QuoteAssetVolume;        // "quoteAssetVolume": "1.0000" - Quote asset volume during this Kline
    uint64_t NumberOfTrades;             // "numberOfTrades": 100 - Number of trades during this Kline
    std::string TakerBuyBaseAssetVolume; // "takerBuyBaseAssetVolume": "500" - Volume of active buy during this Kline
    std::string TakerBuyQuoteAssetVolume;// "takerBuyQuoteAssetVolume": "0.500" - Quote asset volume of active buy during this Kline

    // Conversion method to convert from KlineResponseWs
    inline static KlineResponseRest fromWs(const KlineResponseWs& ws);

    // Serialization/Deserialization methods
    inline static KlineResponseRest deserializeFromJson(const nlohmann::json& j);
    inline static std::vector<KlineResponseRest> parseKlineRest(const std::string& klineString);
};

class KlineResponseWs {
public:
    // kline info
    std::string EventType;       // "e": "kline" - Event type
    int64_t EventTime;           // "E": 1672515782136 - Event time
    std::string Symbol;          // "s": "BNBBTC" - Trading pair
    int64_t StartTime;           // "t": 1672515780000 - Start time of this Kline
    int64_t EndTime;             // "T": 1672515839999 - End time of this Kline
    std::string Interval;        // "i": "1m" - Kline interval
    int64_t FirstTradeID;        // "f": 100 - First trade ID during this Kline
    int64_t LastTradeID;         // "L": 200 - Last trade ID during this Kline
    std::string Open;            // "o": "0.0010" - Opening price
    std::string Close;           // "c": "0.0020" - Closing price
    std::string High;            // "h": "0.0025" - Highest price during this Kline
    std::string Low;             // "l": "0.0015" - Lowest price during this Kline
    std::string Volume;          // "v": "1000" - Volume during this Kline
    int64_t TradeNum;            // "n": 100 - Number of trades during this Kline
    bool IsFinal;                // "x": false - Whether this Kline is final
    std::string QuoteVolume;     // "q": "1.0000" - Quote asset volume during this Kline
    std::string ActiveBuyVolume; // "V": "500" - Volume of active buy during this Kline
    std::string ActiveBuyQuoteVolume; // "Q": "0.500" - Quote asset volume of active buy during this Kline
    int64_t IgnoreParam;         // "B": "123456" - Ignore this parameter

    // Conversion method to convert from KlineResponseRest
    inline static KlineResponseWs fromRest(const KlineResponseRest& rest);

    // Serialization/Deserialization methods
    inline static KlineResponseWs deserializeFromJson(const nlohmann::json& j);
    inline static KlineResponseWs deserializeFromJsonRestArrary(const nlohmann::json& j);
    inline static void parseKlineWs(const std::string& klineString, std::string symbol, std::string interval, std::vector<KlineResponseWs>& klines);
    inline static nlohmann::json serializeToJson(const KlineResponseWs& kline);
};

// ------------------ Inline KlineResponseRest ------------------

inline KlineResponseRest KlineResponseRest::fromWs(const KlineResponseWs& ws) {
    KlineResponseRest rest;
    rest.OpenTime = ws.StartTime;
    rest.CloseTime = ws.EndTime;
    rest.Open = ws.Open;
    rest.Close = ws.Close;
    rest.High = ws.High;
    rest.Low = ws.Low;
    rest.Volume = ws.Volume;
    rest.NumberOfTrades = ws.TradeNum;
    rest.QuoteAssetVolume = ws.QuoteVolume;
    rest.TakerBuyBaseAssetVolume = ws.ActiveBuyVolume;
    rest.TakerBuyQuoteAssetVolume = ws.ActiveBuyQuoteVolume;
    return rest;
}

inline KlineResponseRest KlineResponseRest::deserializeFromJson(const nlohmann::json& j) {
    KlineResponseRest kline;
    try {
        if (j.size() < 11) {
            throw std::runtime_error("Invalid Kline data size");
        }
        kline.OpenTime = j.at(0).get<uint64_t>();
        kline.Open = j.at(1).get<std::string>();
        kline.High = j.at(2).get<std::string>();
        kline.Low = j.at(3).get<std::string>();
        kline.Close = j.at(4).get<std::string>();
        kline.Volume = j.at(5).get<std::string>();
        kline.CloseTime = j.at(6).get<uint64_t>();
        kline.QuoteAssetVolume = j.at(7).get<std::string>();
        kline.NumberOfTrades = j.at(8).get<uint64_t>();
        kline.TakerBuyBaseAssetVolume = j.at(9).get<std::string>();
        kline.TakerBuyQuoteAssetVolume = j.at(10).get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        std::cerr << "Error parsing Kline data: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "General error: " << e.what() << std::endl;
    }
    return kline;
}

inline std::vector<KlineResponseRest> KlineResponseRest::parseKlineRest(const std::string& klineString) {
    // Parse string to JSON
    auto jsonData = nlohmann::json::parse(klineString);
    
    std::vector<KlineResponseRest> klines;
    for (const auto& klineJson : jsonData) {
        klines.push_back(KlineResponseRest::deserializeFromJson(klineJson));
    }
    return klines;
}

// ------------------ Inline KlineResponseWs ------------------

inline KlineResponseWs KlineResponseWs::fromRest(const KlineResponseRest& rest) {
    KlineResponseWs ws;
    ws.StartTime = rest.OpenTime;
    ws.EndTime = rest.CloseTime;
    ws.Open = rest.Open;
    ws.Close = rest.Close;
    ws.High = rest.High;
    ws.Low = rest.Low;
    ws.Volume = rest.Volume;
    ws.TradeNum = rest.NumberOfTrades;
    ws.QuoteVolume = rest.QuoteAssetVolume;
    ws.ActiveBuyVolume = rest.TakerBuyBaseAssetVolume;
    ws.ActiveBuyQuoteVolume = rest.TakerBuyQuoteAssetVolume;
    return ws;
}

// Serialization/Deserialization methods
inline KlineResponseWs KlineResponseWs::deserializeFromJson(const nlohmann::json& j) {
    KlineResponseWs kline;
    
    try{

        if (j.contains("e") && j["e"].is_string()) {
            kline.EventType = j["e"].get<std::string>();
            // std::strncpy(kline.EventType, j["e"].get<std::string>().c_str(), sizeof(kline.EventType));
        } else {
            std::cerr << "Missing or invalid type for 'e'" << std::endl;
        }

        if (j.contains("E") && j["E"].is_number_integer()) {
            kline.EventTime = j["E"].get<int64_t>();
        } else {
            std::cerr << "Missing or invalid type for 'E'" << std::endl;
        }

        if (j.contains("s") && j["s"].is_string()) {
            kline.Symbol = j["s"].get<std::string>();
            // std::strncpy(kline.Symbol, j["s"].get<std::string>().c_str(), sizeof(kline.Symbol));
        } else {
            std::cerr << "Missing or invalid type for 's'" << std::endl;
        }

        if (j.contains("k") && j["k"].is_object()) {
            const auto& k = j["k"];

            if (k.contains("t") && k["t"].is_number_integer()) {
                kline.StartTime = k["t"].get<int64_t>();
            } else {
                std::cerr << "Missing or invalid type for 't'" << std::endl;
            }

            if (k.contains("T") && k["T"].is_number_integer()) {
                kline.EndTime = k["T"].get<int64_t>();
            } else {
                std::cerr << "Missing or invalid type for 'T'" << std::endl;
            }

            if (k.contains("i") && k["i"].is_string()) {
                kline.Interval = k["i"].get<std::string>();
                // std::strncpy(kline.Interval, k["i"].get<std::string>().c_str(), sizeof(kline.Interval));
            } else {
                std::cerr << "Missing or invalid type for 'i'" << std::endl;
            }

            if (k.contains("f") && k["f"].is_number_integer()) {
                kline.FirstTradeID = k["f"].get<int64_t>();
            } else {
                std::cerr << "Missing or invalid type for 'f'" << std::endl;
            }

            if (k.contains("L") && k["L"].is_number_integer()) {
                kline.LastTradeID = k["L"].get<int64_t>();
            } else {
                std::cerr << "Missing or invalid type for 'L'" << std::endl;
            }

            if (k.contains("o") && k["o"].is_string()) {
                kline.Open = k["o"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'o'" << std::endl;
            }

            if (k.contains("c") && k["c"].is_string()) {
                kline.Close = k["c"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'c'" << std::endl;
            }

            if (k.contains("h") && k["h"].is_string()) {
                kline.High = k["h"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'h'" << std::endl;
            }

            if (k.contains("l") && k["l"].is_string()) {
                kline.Low = k["l"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'l'" << std::endl;
            }

            if (k.contains("v") && k["v"].is_string()) {
                kline.Volume = k["v"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'v'" << std::endl;
            }

            if (k.contains("n") && k["n"].is_number_integer()) {
                kline.TradeNum = k["n"].get<int64_t>();
            } else {
                std::cerr << "Missing or invalid type for 'n'" << std::endl;
            }

            if (k.contains("x") && k["x"].is_boolean()) {
                kline.IsFinal = k["x"].get<bool>();
            } else {
                std::cerr << "Missing or invalid type for 'x'" << std::endl;
            }

            if (k.contains("q") && k["q"].is_string()) {
                kline.QuoteVolume = k["q"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'q'" << std::endl;
            }

            if (k.contains("V") && k["V"].is_string()) {
                kline.ActiveBuyVolume = k["V"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'V'" << std::endl;
            }

            if (k.contains("Q") && k["Q"].is_string()) {
                kline.ActiveBuyQuoteVolume = k["Q"].get<std::string>();
            } else {
                std::cerr << "Missing or invalid type for 'Q'" << std::endl;
            }

            // if (k.contains("B") && k["B"].is_number_integer()) {
            //     kline.IgnoreParam = k["B"].get<int64_t>();
            // } else {
            //     std::cerr << "Missing or invalid type for 'B'" << std::endl;
            // }
        } else {
            std::cerr << "Missing or invalid type for 'k'" << std::endl;
        }

    }
    catch(const std::exception& e){
        std::cerr << e.what() << '\n';
    }

    return kline;
}

inline KlineResponseWs KlineResponseWs::deserializeFromJsonRestArrary(const nlohmann::json& j) {
        KlineResponseWs kline;
        try {
            if (j.size() < 11) {
                throw std::runtime_error("Invalid Kline data size");
            }
            
            kline.EventType = "kline";
            kline.StartTime = j.at(0).get<uint64_t>();
            kline.Open = j.at(1).get<std::string>();
            kline.High = j.at(2).get<std::string>();
            kline.Low = j.at(3).get<std::string>();
            kline.Close = j.at(4).get<std::string>();
            kline.Volume = j.at(5).get<std::string>();
            kline.EndTime = j.at(6).get<uint64_t>();
            kline.QuoteVolume = j.at(7).get<std::string>();
            kline.TradeNum = j.at(8).get<uint64_t>();
            kline.ActiveBuyVolume = j.at(9).get<std::string>();
            kline.ActiveBuyQuoteVolume = j.at(10).get<std::string>();
            kline.IsFinal = true;

            kline.EventTime = kline.StartTime;
            kline.FirstTradeID = 0;
            kline.LastTradeID = 0;
        } catch (const nlohmann::json::exception& e) {
            std::cerr << "Error parsing Kline data: " << e.what() << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "General error: " << e.what() << std::endl;
        }
        return kline;
}

inline void KlineResponseWs::parseKlineWs(const std::string& klineString, std::string symbol, std::string interval, std::vector<KlineResponseWs>& klines) {
    // Parse string to JSON
    auto jsonData = nlohmann::json::parse(klineString);
    for (const auto& klineJson : jsonData) {
        auto kline = KlineResponseWs::deserializeFromJsonRestArrary(klineJson); 
        kline.Symbol = symbol;
        kline.Interval = interval;
        klines.push_back(kline);
    }
}

inline nlohmann::json KlineResponseWs::serializeToJson(const KlineResponseWs& kline) {
    nlohmann::json j;
    j["e"] = kline.EventType;
    j["E"] = kline.EventTime;
    j["s"] = kline.Symbol;
    j["k"] = {
        {"t", kline.StartTime},
        {"T", kline.EndTime},
        {"s", kline.Symbol},
        {"i", kline.Interval},
        {"f", kline.FirstTradeID},
        {"L", kline.LastTradeID},
        {"o", kline.Open},
        {"c", kline.Close},
        {"h", kline.High},
        {"l", kline.Low},
        {"v", kline.Volume},
        {"n", kline.TradeNum},
        {"x", kline.IsFinal},
        {"q", kline.QuoteVolume},
        {"V", kline.ActiveBuyVolume},
        {"Q", kline.ActiveBuyQuoteVolume},
        // {"B", kline.IgnoreParam}
    };
    return j;
}

#endif
