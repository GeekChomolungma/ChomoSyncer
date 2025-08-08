#include "indicator_manager.h"
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <iostream>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

const std::string DB_INDICATOR = "indicators";

IndicatorManager::IndicatorManager(MongoManager& mongo) : mongo_(mongo) {}

std::string IndicatorManager::makeSymbolKey(const std::string& symbol, const std::string& interval) {
    // Convert symbol to uppercase
    std::string symbolUpper = symbol;
    std::transform(symbolUpper.begin(), symbolUpper.end(), symbolUpper.begin(), ::toupper);

    return symbolUpper + "_" + interval + "_Binance";
}

std::string IndicatorManager::makeSymbolKeyIndicatorName(const std::string& indicatorName, const std::string& symbol, const std::string& interval) {
    std::string symbolUpper = symbol;
    std::transform(symbolUpper.begin(), symbolUpper.end(), symbolUpper.begin(), ::toupper);

    return indicatorName + "_" + symbolUpper + "_" + interval + "_Binance";
}

void IndicatorManager::loadIndicators(std::vector<std::string> marketSymbols, std::vector<std::string> marketIntervals) {
    // here we can load different indicators based on the market symbols and intervals
    for (const auto& symbol : marketSymbols) {
        for (const auto& interval : marketIntervals) {
            std::cout << "Loaded indicator for symbol: " << symbol << ", interval: " << interval << std::endl;
            // Create a unique key for the symbol and interval
            std::string key = makeSymbolKey(symbol, interval);
            if (calculatorsBySymbol_.find(key) == calculatorsBySymbol_.end()) {
                // currently, we just create RSI calculators for each symbol and interval
                std::vector<std::shared_ptr<IndicatorCalculator>> calcs;
                calcs.emplace_back(std::make_shared<RSICalculator>(14));
                calculatorsBySymbol_[key] = calcs;
                
                std::cout << "Created RSI calculator, Key: " << key << std::endl;

                // Later, we can add more indicators like MACD, Bollinger Bands, etc.
            }
            else {
                std::cout << "Indicator for symbol: " << symbol << ", interval: " << interval << " already exists." << std::endl;
            }
        }
    }
}

void IndicatorManager::prepare(const std::string& dbName, const std::string& symbol, const std::string& interval, int historyWindow) {
    std::cout << "Pre-retrieving history klines from db for indicators, symbol : " << symbol << ", interval: " << interval << std::endl;
    std::string key = makeSymbolKey(symbol, interval);

    std::vector<Kline> window;
    int64_t start_time = 0;
    int64_t end_time = 0;
    mongo_.GetLatestSyncedTime(dbName, key, start_time, end_time);
    mongo_.GetLatestSyncedKlines(end_time, historyWindow, dbName, key, window);

    for (const auto& k : window) {
        for (auto& calc : calculatorsBySymbol_[key]) {
            calc->update(k);
        }
    }
}

void IndicatorManager::processNewKline(const Kline& k) {
    std::string key = makeSymbolKey(k.Symbol, k.Interval);

    for (auto& calc : calculatorsBySymbol_[key]) {
        calc->update(k);
        auto resultOpt = calc->getLatest();
        if (resultOpt.has_value()) {
            persistIndicatorResult(resultOpt.value());
        }
    }
}

void IndicatorManager::persistIndicatorResult(const IndicatorResult& result) {
    bsoncxx::builder::basic::document doc;
    doc.append(
        kvp("starttime", result.startTime),
        kvp("endtime", result.endTime),
        kvp("name", result.name),
        kvp("period", result.period)
    );

    for (const auto& [key, val] : result.values) {
        doc.append(kvp(key, val));
    }

    std::string colName = makeSymbolKeyIndicatorName(result.name, result.symbol, result.interval);
    mongo_.WriteIndicator(DB_INDICATOR, colName, doc.view());
}
