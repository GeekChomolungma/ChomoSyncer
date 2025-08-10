#pragma once
#include <string>
#include <unordered_map>
#include <optional>

// a common structure for indicator results
struct IndicatorState {
    std::string name; // like "RSI", "MACD", etc.
    std::string symbol;
    std::string interval;

    int64_t startTime = 0;
    int64_t endTime = 0;
    int64_t period = 0; // period of the indicator, e.g., 14 for RSI
    std::unordered_map<std::string, double> values; // key: value pairs, e.g., {"RSI": 70.5}
};
