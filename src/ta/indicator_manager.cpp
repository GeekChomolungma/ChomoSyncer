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

std::string IndicatorManager::makeSymbolKeyIndicatorName(const std::string& indicatorName, const std::string& period, const std::string& symbol, const std::string& interval) {
    std::string symbolUpper = symbol;
    std::transform(symbolUpper.begin(), symbolUpper.end(), symbolUpper.begin(), ::toupper);

    return indicatorName + "_" + period + "_" + symbolUpper + "_" + interval + "_Binance";
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

void IndicatorManager::loadStates(const std::string& originDB, std::vector<std::string> marketSymbols, std::vector<std::string> marketIntervals, int historyWindow) {
    for (const auto& symbol : marketSymbols) {
        for (const auto& interval : marketIntervals) {
            std::string key = makeSymbolKey(symbol, interval);
            
            // step 1: fetch the latest origin klines in case some indicator need.
            std::vector<Kline> klines_window;
            int64_t start_time = 0;
            int64_t end_time = 0;
            mongo_.GetLatestSyncedTime(originDB, key, start_time, end_time);
            mongo_.GetLatestSyncedKlines(end_time, historyWindow, originDB, key, klines_window);

            // step 2: get indicator state from the specific db, and initial each indicator state
            auto indicatorList = calculatorsBySymbol_[key];
            for (const auto& indicatorInst : indicatorList) {
                auto colName = makeSymbolKeyIndicatorName(indicatorInst->name(), indicatorInst->period(), symbol, interval);
                auto is_ptr = getLatestIndicatorState(DB_INDICATOR, colName);
                if (is_ptr) {
                    std::cout << "Loaded indicator state for " << colName << std::endl;
                    if (!indicatorInst->loadState(*is_ptr)) {
                        std::cerr << "Failed to load state for indicator: " << indicatorInst->name() << std::endl;
                    }
                }
                else {
                    std::cout << "No previous state found for " << colName << ", initializing new state." << std::endl;
                }
            }
        }
    }
}

void IndicatorManager::processNewKline(const Kline& k) {
    std::string key = makeSymbolKey(k.Symbol, k.Interval);

    for (auto& calc : calculatorsBySymbol_[key]) {
        if (calc->update(k)) {
            if (auto r = calc->getLatest(); r.has_value()) {
                persistIndicatorState(*r);
            }
        }
    }
}

void IndicatorManager::persistIndicatorState(const IndicatorState& result) {
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

    std::string colName = makeSymbolKeyIndicatorName(result.name, std::to_string(result.period), result.symbol, result.interval);
    mongo_.WriteIndicatorState(DB_INDICATOR, colName, doc.view());
}

std::shared_ptr<IndicatorState> IndicatorManager::getLatestIndicatorState(const std::string& dbName, const std::string& colName) {
    auto is = mongo_.ReadIndicatorLatestState(dbName, colName);
    if (is.has_value()) {
        std::cout << "Loaded latest indicator state for " << colName << std::endl;
        return std::make_shared<IndicatorState>(is.value());
    }
    else {
        std::cout << "No latest indicator state found for " << colName << std::endl;
        return nullptr;
    }
}
