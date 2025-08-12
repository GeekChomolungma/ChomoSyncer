# ChomoSyncer

## Overview

ChomoSyncer is a high-performance market data synchronizer for Binance. It retrieves real-time cryptocurrency candlestick (Kline) data and distributes it via Redis while storing it in MongoDB. It supports a multi-threaded and asynchronous architecture, making it suitable for quantitative trading, data analysis, and backtesting scenarios.

![ChomoSyncer Workflow](docs/images/workflow.jpg)


## Features

* **Real-time market data sync**: Combines WebSocket and REST API to fetch multi-symbol Kline data with low latency.
* **Multi-timeframe support**: Handles multiple intervals such as 1m, 15m, 1h, and 4h simultaneously.
* **Data distribution & persistence**: Uses Redis Stream for real-time data distribution and MongoDB for long-term storage.

## Quick Start

```bash
git clone https://github.com/GeekChomolungma/ChomoSyncer.git
cd ChomoSyncer
```

## Installation

### 0. Install MongoDB & Redis

ChomoSyncer depends on MongoDB and Redis for storage and message queue functionality. Please install and start them locally or on a remote server before running ChomoSyncer:

* **MongoDB**: [Official Installation Guide](https://www.mongodb.com/docs/manual/installation/)
* **Redis**: [Official Installation Guide](https://redis.io/docs/latest/operate/rc/rc-quickstart/)

Make sure both services are accessible using the connection parameters defined in your configuration file.

### 1. Install ChomoSyncer

#### Windows
Refer to the [Windows installation guide](docs/install_guide_win.md) for detailed steps.

#### Ubuntu
Refer to the [Ubuntu installation guide](docs/install_guide_ubu.md) for detailed steps.

## Usage

1. Edit `config.ini` to configure Binance API, Redis, MongoDB, and other parameters(see conmments in config.ini)
2. Start the program:

   ```bash
   ./ChomoSyncer
   ```
3. The program will start syncing market data, distributing it through Redis, and persisting it to MongoDB.

## Contributing

Contributions are welcome! You can submit PRs, report bugs, or suggest improvements. Please ensure the code builds successfully and follows the project's code style and commit guidelines before submission.

## License

MIT License
