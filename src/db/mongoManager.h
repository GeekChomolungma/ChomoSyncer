#ifndef MONGOMANAGER_H
#define MONGOMANAGER_H

#include <vector>
#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/exception/exception.hpp>
#include <optional>
#include <unordered_set>

#include "dtos/kline.h"
#include "dtos/settlementItem.h"
#include "ta/indicator_state.h"

using bsoncxx::to_json;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::basic::kvp;

class MongoManager {
public:
    MongoManager(std::string uriStr);
    
    int64_t GetSynedFlag(std::string dbName, std::string colName);
    
    bool ParseKline(const bsoncxx::v_noabi::document::view& doc, Kline& klineInst);

    void GetKline(int64_t startTime, int64_t endTime, int limit, int sortOrder, std::string dbName, std::string colName, std::vector<Kline>& targetKlineList);

    void GetLatestSyncedKlines(int64_t endTime, int limit, std::string dbName, std::string colName, std::vector<Kline>& fetchedDataPerCol);

    void GetLatestSyncedTime(std::string dbName, std::string colName, int64_t& latestSyncedStartTime, int64_t& latestSyncedEndTime);

    void WriteClosedKlines(std::string dbName, std::vector<KlineResponseWs>& rawData);

    std::optional<IndicatorState> ReadIndicatorLatestState(const std::string& dbName, const std::string& colName);

    static inline bool is_fixed_field(std::string_view k) {
        static const std::unordered_set<std::string> fixed = {
            "_id", "starttime", "endtime", "name", "period", "symbol", "interval"
        };
        return fixed.count(std::string(k)) > 0;
    }

    static inline bool element_to_double(const bsoncxx::document::element& el, double& out) {
        using bsoncxx::type;
        switch (el.type()) {
        case type::k_double:     out = el.get_double().value; return true;
        case type::k_int32:      out = static_cast<double>(el.get_int32().value); return true;
        case type::k_int64:      out = static_cast<double>(el.get_int64().value); return true;
        case type::k_decimal128: {
            auto dec = el.get_decimal128().value;
            try { out = std::stod(dec.to_string()); }
            catch (...) { return false; }
            return true;
        }
        default: return false;
        }
    }

    void WriteIndicatorState(std::string dbName, std::string colName, const bsoncxx::v_noabi::document::view& doc);

    void BulkWriteClosedKlines(std::string dbName, std::string colName, std::vector<KlineResponseWs>& rawData);

    std::string SetSettlementItems(std::string dbName, std::string colName,SettlementItem& data);

    void GetKlineUpdate(std::string dbName, std::string colName, std::vector<Kline>& PreviousTwoKlines); // polling

    // mongo should be deployed in replica mode
    void WatchKlineUpdate(std::string dbName, std::string colName, std::vector<Kline>& PreviousTwoKlines);

private:
    std::string uriStr;
    mongocxx::instance inst;
    //mongocxx::client mongoClient;
    mongocxx::pool mongoPool;
};

#endif