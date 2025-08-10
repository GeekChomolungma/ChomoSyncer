#pragma once

#include <cctype>
#include <cmath>
#include <limits>
#include <algorithm>
#include <utility>
#include <deque>

#include "indicator_calculator.h"


class RSICalculator : public IndicatorCalculator {
public:
    explicit RSICalculator(int period);

    bool loadState(const IndicatorState& is) override;
    bool update(const Kline& k) override;

    std::string name() const override { return "rsi"; }
    std::string period() const override { return std::to_string(period_); }

    std::optional<IndicatorState> getLatest() const override;

private:
    int period_;
    bool initialized_ = false;
    bool seeded_{ false };           // if enter into RUN state
    int  warmup_count_{ 0 };         // still warm up if count < period_

    double prev_close_{ 0.0 };
    double sum_gain_{ 0.0 };         // warmup
    double sum_loss_{ 0.0 };         // warmup
    double avg_gain_{ 0.0 };         // RUN
    double avg_loss_{ 0.0 };         // RUN
    
    // Idempotence: avoding duplicate calculations
    int64_t last_start_ = -1;

    std::optional<IndicatorState> latest_result_;

    static inline bool fetch_num(const IndicatorState& is, const char* key, double& out) {
        auto it = is.values.find(key);
        if (it == is.values.end()) return false;
        out = it->second;
        return std::isfinite(out);
    }
};
