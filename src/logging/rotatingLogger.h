#pragma once

#include <cstddef>
#include <fstream>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <streambuf>
#include <string>

class RotatingLogBuffer : public std::streambuf {
public:
    RotatingLogBuffer(std::string logDir, std::string baseName, std::size_t maxBytes);
    ~RotatingLogBuffer() override;

protected:
    int overflow(int ch) override;
    std::streamsize xsputn(const char* s, std::streamsize n) override;
    int sync() override;

private:
    void openNewFile();
    void rotateIfNeeded(std::size_t incomingBytes);
    std::string makeLogPath() const;
    static std::string makeTimestamp();

    std::string logDir_;
    std::string baseName_;
    std::size_t maxBytes_;
    std::size_t currentBytes_ = 0;
    unsigned int sequence_ = 0;
    std::ofstream file_;
    std::mutex mutex_;
};

class RotatingLogger {
public:
    RotatingLogger(const std::string& logDir, std::size_t maxBytes);
    ~RotatingLogger();

    RotatingLogger(const RotatingLogger&) = delete;
    RotatingLogger& operator=(const RotatingLogger&) = delete;

private:
    std::unique_ptr<RotatingLogBuffer> buffer_;
    std::streambuf* oldCout_ = nullptr;
    std::streambuf* oldCerr_ = nullptr;
};
