#include <iostream>
#include <sstream>
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

    std::string getUri() const{
        return pt.get<std::string>("database.uri");
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

private:
    boost::property_tree::ptree pt;
};