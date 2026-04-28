#include <hiredis/hiredis.h>
#include "dataSync/exBinance.h"
#include "logging/rotatingLogger.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>

std::atomic<bool> keepRunning{ true };

int main() {
    // init redis, localhost and default port
    // create shared ptr for BinanceDataSync
    Config cfg("config.ini");
    const auto logMaxBytes = cfg.getLogMaxFileSizeMb() * 1024ULL * 1024ULL;
    RotatingLogger logger(cfg.getLogDir(), static_cast<std::size_t>(logMaxBytes));

    std::cout << "ChomoSyncer started. Logs are written to directory: "
              << cfg.getLogDir() << std::endl;

    std::shared_ptr<BinanceDataSync> binanceDataSync = std::make_shared<BinanceDataSync>("config.ini");
    binanceDataSync->start();
    return 0;
}
