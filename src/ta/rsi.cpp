#include "rsi.h"
#include <cmath>

RSICalculator::RSICalculator(int period) : period_(period) {}

bool RSICalculator::loadState(const IndicatorState& is) {
    if (is.name != "rsi" || is.period != period_) {
        std::cerr << "[RSI] loadState mismatch: name=" << is.name
            << " period=" << is.period << " expect=" << period_ << "\n";
        return false;
    }

    double seeded_flag = 0.0;
    if (!fetch_num(is, "prev_close", prev_close_)) {
        std::cerr << "[RSI] loadState missing/invalid prev_close\n";
        return false;
    }

    if (!fetch_num(is, "seeded", seeded_flag)) {
        // compatibility for old db, if no seeded but avg_, then set seeded to 1.0
        double tmp1 = 0, tmp2 = 0;
        if (fetch_num(is, "avg_gain", tmp1) && fetch_num(is, "avg_loss", tmp2)) {
            seeded_flag = 1.0;
        }
        else {
            seeded_flag = 0.0;
        }
    }
    seeded_ = (seeded_flag != 0.0);

    initialized_ = true;
    last_start_ = is.startTime;

    if (seeded_) {
        // RUN£ºshould has avg_
        if (!fetch_num(is, "avg_gain", avg_gain_) ||
            !fetch_num(is, "avg_loss", avg_loss_)) {
            std::cerr << "[RSI] loadState: seeded but avg_* missing/invalid\n";
            return false;
        }
        // RUN state, sum_ and warmp_count are not necessary
        sum_gain_ = sum_loss_ = 0.0;
        warmup_count_ = period_; // consistence somewhat
    }
    else {
        // WARMUP state£ºsum_* + warmup_count
        if (!fetch_num(is, "sum_gain", sum_gain_) ||
            !fetch_num(is, "sum_loss", sum_loss_)) {
            std::cerr << "[RSI] loadState: warmup but sum_* missing/invalid\n";
            return false;
        }
        double cnt = 0.0;
        if (!fetch_num(is, "warmup_count", cnt)) {
            std::cerr << "[RSI] loadState: warmup_count missing\n";
            return false;
        }
        warmup_count_ = static_cast<int>(cnt);
        if (warmup_count_ < 0 || warmup_count_ > period_) {
            std::cerr << "[RSI] loadState: warmup_count out of range\n";
            return false;
        }
        avg_gain_ = avg_loss_ = 0.0;
    }

    std::cout << "[RSI] loadState OK: seeded=" << seeded_
        << " warmup_count=" << warmup_count_
        << " prev_close=" << prev_close_ << "\n";
    return true;
}

bool RSICalculator::update(const Kline& k) {
    if (!k.IsFinal) return false;

    // Idempotence: avoid duplicate calculations
    if (last_start_ >= 0 && k.StartTime <= last_start_) {
        return false;
    }

    if (!initialized_) {
        prev_close_ = k.Close;
        initialized_ = true;
        last_start_ = k.StartTime;
        return false;
    }

    double change = k.Close - prev_close_;
    double gain = change > 0 ? change : 0;
    double loss = change < 0 ? -change : 0;

    if (!seeded_) {
        // cold start in period
        sum_gain_ += gain;
        sum_loss_ += loss;
        warmup_count_ += 1;

        if (warmup_count_ >= period_) {
            // into RUN state, means finshed warm-up
            avg_gain_ = sum_gain_ / period_;
            avg_loss_ = sum_loss_ / period_;
            seeded_ = true;
        }
    }
    else {
        // RUN£ºWilder window
        avg_gain_ = (avg_gain_ * (period_ - 1) + gain) / period_;
        avg_loss_ = (avg_loss_ * (period_ - 1) + loss) / period_;
    }

    // generate RSI
    if (seeded_) {
        const double rsiVal = (avg_loss_ == 0.0)
            ? 100.0
            : 100.0 - (100.0 / (1.0 + (avg_gain_ / avg_loss_)));

        IndicatorState result;
        result.name = "rsi";
        result.symbol = k.Symbol;
        result.interval = k.Interval;
        result.period = period_;
        result.startTime = k.StartTime;
        result.endTime = k.EndTime;

        result.values["rsi"] = rsiVal;
        result.values["avg_gain"] = avg_gain_;
        result.values["avg_loss"] = avg_loss_;
        result.values["prev_close"] = k.Close;
        result.values["seeded"] = 1.0;          // means RUN

        // optional record warmup count
        result.values["warmup_count"] = 0.0;
        result.values["sum_gain"] = 0.0;
        result.values["sum_loss"] = 0.0;

        latest_result_ = std::move(result);
    }
    else {
        // still in warm-up
        IndicatorState result;
        result.name = "rsi";
        result.symbol = k.Symbol;
        result.interval = k.Interval;
        result.period = period_;
        result.startTime = k.StartTime;
        result.endTime = k.EndTime;

        result.values["prev_close"] = k.Close;
        result.values["seeded"] = 0.0;

        result.values["warmup_count"] = static_cast<double>(warmup_count_);
        result.values["sum_gain"] = sum_gain_;
        result.values["sum_loss"] = sum_loss_;

        latest_result_ = std::move(result);
    }

    prev_close_ = k.Close;
    last_start_ = k.StartTime;   // update the last processed start time
    return latest_result_.has_value();
}

std::optional<IndicatorState> RSICalculator::getLatest() const {
    return latest_result_;
}
