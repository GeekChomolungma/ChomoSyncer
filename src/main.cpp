#include <hiredis/hiredis.h>
#include "dataSync/exBinance.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>

std::atomic<bool> keepRunning{ true };

int main() {
    BinanceDataSync binanceDataSync;

    std::thread prodThread(std::bind(&BinanceDataSync::handle_market_data, &binanceDataSync));
    std::thread consThread(std::bind(&BinanceDataSync::handle_rest_operations, &binanceDataSync));

    std::this_thread::sleep_for(std::chrono::seconds(10)); // Let it run for 10 seconds
    keepRunning = false;

    prodThread.join();
    consThread.join();

    return 0;
}
