#include <hiredis/hiredis.h>
#include "dataSync/exBinance.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include "config/config.h"

std::atomic<bool> keepRunning{ true };

int main() {
    
    Config cfg("config.ini");
    const std::vector<std::string> allSymbols = cfg.getMarketSubInfo("marketsub.symbols");
    const std::vector<std::string> allIntervals = cfg.getMarketSubInfo("marketsub.intervals");
    std::cout << "allSymbols: " << allSymbols.size() << "allIntervals" << allIntervals.size() << std::endl;

    // init redis, localhost and default port
    BinanceDataSync binanceDataSync("mongodb://localhost:27017", "localhost", 6379);
    binanceDataSync.start();

    return 0;
}
