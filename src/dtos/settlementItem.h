#ifndef SETTLEITEM_H
#define SETTLEITEM_H

#include <cstdint>
#include <string>

class SettlementItem {
public:
    // kline info
    std::string Id;
    std::string PreviousId;
    int64_t StartTime;
    int64_t EndTime;
    char Symbol[16];
    char Interval[16];
    int     Action; // 0 is none, 1 is sell, 2 is buy
    int64_t TradeID;
    int64_t ExecTime;
    float   ExecPrice;
    float   ExecVolume;
    float   ProfitValue;
    float   SumValue;
    float   SumAmout;
};
#endif
