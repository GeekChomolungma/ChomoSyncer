#ifndef KLINE_H
#define KLINE_H

#include <cstdint>

class Kline {
public:
    // kline info
    char Id[12];
    int64_t StartTime;
    int64_t EndTime;
    char Symbol[16];
    char Interval[16];
    int64_t FirstTradeID;
    int64_t LastTradeID;
    double  Open;
    double  Close;
    double  High;
    double  Low;
    double  Volume;
    int64_t  TradeNum;
    bool IsFinal;
    double  QuoteVolume;
    double  ActiveBuyVolume;
    double  ActiveBuyQuoteVolume;
    double  TrueRange;

    // SuperTrend Indicator
    // need to calculate if length is change
    double  AveTrueRange;
    double  SuperTrendValue;
    double  StUp;
    double  StDown;
    int     STDirection; // -1 1
    int     Action; // 0 is none, 1 is sell, 2 is buy
};
#endif
