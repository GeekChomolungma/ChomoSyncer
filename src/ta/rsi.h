#pragma once
#include "indicator_calculator.h"
#include <deque>

class RSICalculator : public IndicatorCalculator {
public:
    explicit RSICalculator(int period);

    void update(const Kline& k) override;
    std::string name() const override { return "rsi"; }

    std::optional<IndicatorResult> getLatest() const override;

private:
    int period_;
    std::deque<double> gain_history_;
    std::deque<double> loss_history_;
    double avg_gain_ = 0.0;
    double avg_loss_ = 0.0;
    bool initialized_ = false;
    double prev_close_ = 0.0;
    std::optional<IndicatorResult> latest_result_;
};
