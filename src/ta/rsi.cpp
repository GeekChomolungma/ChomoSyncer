#include "rsi.h"
#include <cmath>

RSICalculator::RSICalculator(int period) : period_(period) {}

bool RSICalculator::update(const Kline& k) {
    if (!k.IsFinal) return false;

    // Idempotence: avoid duplicate calculations
    if (last_start_ >= 0 && k.StartTime <= last_start_) {
        return false;
    }

    if (!initialized_) {
        prev_close_ = k.Close;
        initialized_ = true;
        return false;
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

        latest_result_ = std::move(result);
    }

    prev_close_ = k.Close;
    last_start_ = k.StartTime;   // update the last processed start time
    return latest_result_.has_value();
}

std::optional<IndicatorResult> RSICalculator::getLatest() const {
    return latest_result_;
}
