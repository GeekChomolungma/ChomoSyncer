#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

class Config {
public:
    Config(const std::string& filename) {
        boost::property_tree::ini_parser::read_ini(filename, pt);
    }

    // database info
    std::string getDatabaseHost() const {
        return pt.get<std::string>("database.host");
    }

    int getDatabasePort() const {
        return pt.get<int>("database.port");
    }

    std::string getDatabaseUsername() const {
        return pt.get<std::string>("database.username");
    }

    std::string getDatabasePassword() const {
        return pt.get<std::string>("database.password");
    }

    std::string getDatabaseUri() const{
        return pt.get<std::string>("database.uri");
    }

    // redis info
    std::string getRedisHost() const {
        return pt.get<std::string>("redis.host");
    }

    int getRedisPort() const {
        return pt.get<int>("redis.port");
    }

    std::string getRedisPassword() const {
        return pt.get<std::string>("redis.password");
    }

    // market info
    // symbols, intervals, with ',' separated
    std::vector<std::string> getMarketSubInfo(std::string target) const {
        std::string input = pt.get<std::string>(target);
        std::stringstream ss(input);

        std::vector<std::string> tokens;
        std::string token;
        
        while (std::getline(ss, token, ',')) {
            tokens.push_back(token);
        }

        return tokens;
    }

    std::string getLogDir() const {
        return pt.get<std::string>("logging.dir", "logs");
    }

    uint64_t getLogMaxFileSizeMb() const {
        return pt.get<uint64_t>("logging.max_file_size_mb", 1024);
    }

    uint64_t getHistoryKlineSyncStartMs() const {
        return pt.get<uint64_t>("history.kline_sync_start_ms", 0);
    }

private:
    boost::property_tree::ptree pt;
};
