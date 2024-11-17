#include <hiredis/hiredis.h>
#include "dataSync/exBinance.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>

std::atomic<bool> keepRunning{ true };

int main() {
    // init redis, localhost and default port
    // create shared ptr for BinanceDataSync
    std::shared_ptr<BinanceDataSync> binanceDataSync = std::make_shared<BinanceDataSync>("config.ini");
    binanceDataSync->start();

    return 0;
}
