#pragma once

#include "indicator_calculator.h"

#include "rsi.h" // or more indicators as needed

#include "db/mongoManager.h"
#include "dtos/kline.h"
#include <vector>
#include <memory>
#include <unordered_map>
#include <string>

// manages multiple indicators, processes new Klines, and persists results to MongoDB
class IndicatorManager {
public:
    IndicatorManager(MongoManager& mongo);

    std::string makeSymbolKey(const std::string& symbol, const std::string& interval);
    std::string makeSymbolKeyIndicatorName(const std::string& indictorName, const std::string& symbol, const std::string& interval);

    void loadIndicators(std::vector<std::string> marketSymbols, std::vector<std::string> marketIntervals);

    // initialize the manager with a database and collection name, and optionally a history window size
    void prepare(const std::string& dbName, const std::string& symbol, const std::string& interval, int historyWindow = 30);

    // new Kline processing
    void processNewKline(const Kline& k);

private:
    MongoManager& mongo_;

    std::unordered_map<std::string, std::vector<std::shared_ptr<IndicatorCalculator>>> calculatorsBySymbol_; // symbol_interval -> calculators
    std::vector<Kline> window_; // klines for the history window
    const int WINDOW_LIMIT = 100;

    // persist the indicator result to MongoDB
    void persistIndicatorResult(const IndicatorResult& result);
};
