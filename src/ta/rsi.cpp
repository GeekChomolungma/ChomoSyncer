#include "rsi.h"
#include <cmath>

RSICalculator::RSICalculator(int period) : period_(period) {}

void RSICalculator::update(const Kline& k) {
    if (!initialized_) {
        prev_close_ = k.Close;
        initialized_ = true;
        return;
    }

    double change = k.Close - prev_close_;
    double gain = change > 0 ? change : 0;
    double loss = change < 0 ? -change : 0;

    gain_history_.push_back(gain);
    loss_history_.push_back(loss);

    if (gain_history_.size() > period_) {
        gain_history_.pop_front();
        loss_history_.pop_front();
    }

    if (gain_history_.size() == period_) {
        if (avg_gain_ == 0.0 && avg_loss_ == 0.0) {
            for (int i = 0; i < period_; ++i) {
                avg_gain_ += gain_history_[i];
                avg_loss_ += loss_history_[i];
            }
            avg_gain_ /= period_;
            avg_loss_ /= period_;
        }
        else {
            avg_gain_ = (avg_gain_ * (period_ - 1) + gain) / period_;
            avg_loss_ = (avg_loss_ * (period_ - 1) + loss) / period_;
        }

        double rsiVal = avg_loss_ == 0.0 ? 100.0 : 100.0 - (100.0 / (1.0 + avg_gain_ / avg_loss_));

        IndicatorResult result;
        result.name = "rsi";
        result.symbol = k.Symbol;
        result.interval = k.Interval;
        result.period = period_;

        result.startTime = k.StartTime;
        result.endTime = k.EndTime;
        result.values["rsi"] = rsiVal;

        latest_result_ = result;
    }

    prev_close_ = k.Close;
}

std::optional<IndicatorResult> RSICalculator::getLatest() const {
    return latest_result_;
}
