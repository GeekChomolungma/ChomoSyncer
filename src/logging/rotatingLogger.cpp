#include "logging/rotatingLogger.h"

#include <chrono>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <utility>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace {
int currentProcessId() {
#ifdef _WIN32
    return _getpid();
#else
    return getpid();
#endif
}
}

RotatingLogBuffer::RotatingLogBuffer(std::string logDir, std::string baseName, std::size_t maxBytes)
    : logDir_(std::move(logDir)),
      baseName_(std::move(baseName)),
      maxBytes_(maxBytes == 0 ? 1024ULL * 1024ULL * 1024ULL : maxBytes) {
    std::filesystem::create_directories(logDir_);
    openNewFile();
}

RotatingLogBuffer::~RotatingLogBuffer() {
    sync();
}

int RotatingLogBuffer::overflow(int ch) {
    if (ch == traits_type::eof()) {
        return traits_type::not_eof(ch);
    }

    const char c = static_cast<char>(ch);
    return xsputn(&c, 1) == 1 ? ch : traits_type::eof();
}

std::streamsize RotatingLogBuffer::xsputn(const char* s, std::streamsize n) {
    if (n <= 0) {
        return 0;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    rotateIfNeeded(static_cast<std::size_t>(n));
    file_.write(s, n);
    if (!file_) {
        return 0;
    }
    currentBytes_ += static_cast<std::size_t>(n);
    return n;
}

int RotatingLogBuffer::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (file_.is_open()) {
        file_.flush();
    }
    return file_ ? 0 : -1;
}

void RotatingLogBuffer::openNewFile() {
    if (file_.is_open()) {
        file_.flush();
        file_.close();
    }

    const std::string path = makeLogPath();
    file_.open(path, std::ios::out | std::ios::app | std::ios::binary);
    if (!file_) {
        throw std::runtime_error("Failed to open log file: " + path);
    }

    currentBytes_ = static_cast<std::size_t>(file_.tellp());
}

void RotatingLogBuffer::rotateIfNeeded(std::size_t incomingBytes) {
    if (currentBytes_ == 0 || currentBytes_ + incomingBytes <= maxBytes_) {
        return;
    }

    ++sequence_;
    openNewFile();
}

std::string RotatingLogBuffer::makeLogPath() const {
    std::ostringstream oss;
    oss << logDir_ << "/" << baseName_ << "_" << makeTimestamp()
        << "_pid" << currentProcessId()
        << "_" << std::setw(4) << std::setfill('0') << sequence_
        << ".log";
    return oss.str();
}

std::string RotatingLogBuffer::makeTimestamp() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t nowTime = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};

#ifdef _WIN32
    localtime_s(&tm, &nowTime);
#else
    localtime_r(&nowTime, &tm);
#endif

    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d_%H%M%S");
    return oss.str();
}

RotatingLogger::RotatingLogger(const std::string& logDir, std::size_t maxBytes)
    : buffer_(std::make_unique<RotatingLogBuffer>(logDir, "ChomoSyncer", maxBytes)) {
    oldCout_ = std::cout.rdbuf(buffer_.get());
    oldCerr_ = std::cerr.rdbuf(buffer_.get());
}

RotatingLogger::~RotatingLogger() {
    std::cout.rdbuf(oldCout_);
    std::cerr.rdbuf(oldCerr_);
}
