#pragma once
#include "dtos/kline.h"
#include "indicator_result.h"

class IndicatorCalculator {
public:
    virtual bool update(const Kline& newKline) = 0;
    // virtual std::optional<IndicatorResult> finalize(Kline& output) = 0; // write the results to output

    virtual std::string name() const = 0; // return the name of the indicator, e.g., "RSI", "MACD", etc.
    // virtual std::string period() const = 0; // return the period of the indicator, e.g., "14" for RSI
    virtual std::optional<IndicatorResult> getLatest() const = 0; // get the latest result without finalizing

    virtual ~IndicatorCalculator() = default;
};
