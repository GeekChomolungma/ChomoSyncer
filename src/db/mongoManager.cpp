#include "db/mongoManager.h"
#include <iomanip> // std::setw, std::setfill
#include <sstream>
#include <iostream>
#include <chrono>
#include <thread>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

MongoManager::MongoManager(const std::string uriStr):uriStr(uriStr), mongoPool(mongocxx::uri{ this->uriStr.c_str() }){
};

int64_t MongoManager::GetSynedFlag(std::string dbName, std::string colName) {
    auto client = this->mongoPool.acquire();
    auto col = (*client)[dbName.c_str()][colName.c_str()];

    try
    {
        // Ping the database.
        //const auto ping_cmd = make_document(kvp("ping", 1));
        //(*client)[dbName.c_str()].run_command(ping_cmd.view());
        //std::cout << "Pinged your deployment. You successfully connected to MongoDB!" << std::endl;

        auto find_one_result = col.find_one({});
        if (find_one_result) {
            auto extractedValue = *find_one_result;
            auto eViewElement = extractedValue["starttime"];
            auto st = eViewElement.get_int64().value;
            //std::ostringstream ss;
            //ss << "Got synced flag time:" << st << "\n" << std::endl;
            //std::cout << ss.str();
            return st;
        }
        else {
            std::cout << "failed to find one!" << std::endl;
            return 0;
        }
    }
    catch (const std::exception& e)
    {
        // Handle errors.
        std::cout << "Exception: " << e.what() << std::endl;
        return 0;
    }
}

bool MongoManager::ParseKline(const bsoncxx::v_noabi::document::view& doc, Kline& klineInst) {
    const auto& oidBytesVec = doc["_id"].get_oid().value.bytes();
    for (size_t i = 0; i < 12; ++i) {
        klineInst.Id[i] = oidBytesVec[i];
    }

    auto klineContent = doc["kline"];
    if (!klineContent) {
        std::ostringstream ss;
        ss << "klineContent is nil. \n" << std::endl;
        std::cout << ss.str();
        return false;
    }

    klineInst.StartTime = klineContent["starttime"].get_int64().value;
    klineInst.EndTime = klineContent["endtime"].get_int64().value;
    bsoncxx::stdx::string_view symbolTmp = klineContent["symbol"].get_string().value;
    std::string symbolStr(symbolTmp);
    bsoncxx::stdx::string_view intervalTmp = klineContent["interval"].get_string().value;
    std::string intervalStr(intervalTmp);

#ifdef _WIN32
    strcpy_s(klineInst.Symbol, symbolStr.c_str());
    strcpy_s(klineInst.Interval, intervalStr.c_str());
#else
    strcpy(klineInst.Symbol, symbolStr.c_str());
    strcpy(klineInst.Interval, intervalStr.c_str());
#endif

    bsoncxx::stdx::string_view openStrTmp = klineContent["open"].get_string().value;
    klineInst.Open = std::stod(std::string(openStrTmp));

    bsoncxx::stdx::string_view closeStrTmp = klineContent["close"].get_string().value;
    klineInst.Close = std::stod(std::string(closeStrTmp));

    bsoncxx::stdx::string_view highStrTmp = klineContent["high"].get_string().value;
    klineInst.High = std::stod(std::string(highStrTmp));

    bsoncxx::stdx::string_view lowStrTmp = klineContent["low"].get_string().value;
    klineInst.Low = std::stod(std::string(lowStrTmp));

    auto trElement = doc["truerange"];
    if (trElement && trElement.type() == bsoncxx::type::k_double) {
        klineInst.TrueRange = trElement.get_double().value;
    }
    else {
        if (trElement && trElement.type() == bsoncxx::type::k_string) {
            bsoncxx::stdx::string_view trTmp = trElement.get_string().value;
            klineInst.TrueRange = std::stod(std::string(trTmp));
        }
        else {
            klineInst.TrueRange = 0.0;
        }
    }

    auto atrElement = doc["avetruerange"];
    if (atrElement && atrElement.type() == bsoncxx::type::k_double) {
        klineInst.AveTrueRange = atrElement.get_double().value;
    }
    else {
        if (atrElement && atrElement.type() == bsoncxx::type::k_string) {
            bsoncxx::stdx::string_view atrTmp = atrElement.get_string().value;
            klineInst.AveTrueRange = std::stod(std::string(atrTmp));
        }
        else {
            klineInst.AveTrueRange = 0.0;
        }
    }

    auto stElement = doc["supertrendvalue"];
    if (stElement && stElement.type() == bsoncxx::type::k_string) {
        bsoncxx::stdx::string_view stTmp = stElement.get_string().value;
        klineInst.SuperTrendValue = std::stod(std::string(stTmp));
    }
    else {
        klineInst.SuperTrendValue = 0.0;
    }

    auto stupElement = doc["stup"];
    if (stupElement && stupElement.type() == bsoncxx::type::k_string) {
        bsoncxx::stdx::string_view stupTmp = stupElement.get_string().value;
        klineInst.StUp = std::stod(std::string(stupTmp));
    }
    else {
        klineInst.StUp = 0.0;
    }

    auto stdownElement = doc["stdown"];
    if (stdownElement && stdownElement.type() == bsoncxx::type::k_string) {
        bsoncxx::stdx::string_view stdownTmp = stdownElement.get_string().value;
        klineInst.StDown = std::stod(std::string(stdownTmp));
    }
    else {
        klineInst.StDown = 0.0;
    }

    auto dirElement = doc["stdirection"];
    if (dirElement && dirElement.type() == bsoncxx::type::k_int32) {
        klineInst.STDirection = dirElement.get_int32().value;
    }
    else {
        klineInst.STDirection = 0;
    }

    auto actElement = doc["action"];
    if (actElement && actElement.type() == bsoncxx::type::k_int32) {
        klineInst.Action = actElement.get_int32().value;
    }
    else {
        klineInst.Action = 0;
    }

    bsoncxx::stdx::string_view volStrTmp = klineContent["volume"].get_string().value;
    klineInst.Volume = std::stod(std::string(volStrTmp));

    klineInst.TradeNum = klineContent["tradenum"].get_int64();
    klineInst.IsFinal = klineContent["isfinal"].get_bool();

    bsoncxx::stdx::string_view qutovStrTmp = klineContent["quotevolume"].get_string().value;
    klineInst.QuoteVolume = std::stod(std::string(qutovStrTmp));

    return true;
}

// sortOrder:
// -1 delince
// 1 arise
void MongoManager::GetKline(int64_t startTime, int64_t endTime, int limit, int sortOrder, std::string dbName, std::string colName, std::vector<Kline>& targetKlineList) {
    auto client = this->mongoPool.acquire();
    auto col = (*client)[dbName.c_str()][colName.c_str()];

    mongocxx::options::find opts;
    opts.sort(make_document(kvp("kline.starttime", sortOrder)));
    opts.limit(limit);

    auto cursor_filtered = 
        col.find( 
            make_document(
                kvp("kline", make_document(kvp("$exists", true))), 
                kvp("kline.starttime", make_document(kvp("$gte", startTime), kvp("$lte", endTime)))
            ), opts);

    for (auto&& doc : cursor_filtered) {        
        Kline klineInst;
        auto existed = this->ParseKline(doc, klineInst);
        if (existed) {
            targetKlineList.push_back(klineInst);
        }
    }
}

void MongoManager::GetLatestSyncedKlines(int64_t endTime, int limit, std::string dbName, std::string colName, std::vector<Kline>& fetchedDataPerCol) {
    auto client = this->mongoPool.acquire();
    auto col = (*client)[dbName.c_str()][colName.c_str()];

    mongocxx::options::find opts;
    opts.sort(make_document(kvp("kline.starttime", -1)));
    opts.limit(limit);

    bsoncxx::builder::basic::document filter;
    if (endTime != 0) {
        filter.append(kvp("kline.starttime", make_document(kvp("$lte", endTime))));
    }
    auto cursor_filtered = col.find(filter.view(), opts);

    for (auto&& doc : cursor_filtered) {
        Kline klineInst;
        auto existed = this->ParseKline(doc, klineInst);
        if (existed) {
            fetchedDataPerCol.push_back(klineInst);
        } 
    }
    std::reverse(fetchedDataPerCol.begin(), fetchedDataPerCol.end());

    std::ostringstream ss;
    ss << "GetLatestSyncedKlines, fetchedDataPerCol size is: " << fetchedDataPerCol.size() << "\n" << std::endl;
    std::cout << ss.str();
}

void MongoManager::BulkWriteByIds(std::string dbName, std::string colName, std::vector<Kline>& rawData) {
    try{
        // locate the coll
        auto client = this->mongoPool.acquire();
        auto db = (*client)[dbName.c_str()];
        auto col = db[colName.c_str()];

        // create bulk
        auto bulk = col.create_bulk_write();
        for (auto& kline : rawData) {
            bsoncxx::builder::basic::document filter_builder, update_builder;
            bsoncxx::oid docID(&kline.Id[0], 12);

            // format tr and atr to string
            std::ostringstream oss1, oss2, oss3, oss4, oss5;
            oss1 << std::fixed << std::setprecision(6) << kline.TrueRange;
            oss2 << std::fixed << std::setprecision(6) << kline.AveTrueRange;
            oss3 << std::fixed << std::setprecision(6) << kline.SuperTrendValue;
            oss4 << std::fixed << std::setprecision(6) << kline.StUp;
            oss5 << std::fixed << std::setprecision(6) << kline.StDown;
            std::string trStr = oss1.str();
            std::string atrStr = oss2.str();
            std::string stStr = oss3.str();
            std::string stUpStr = oss4.str();
            std::string stDownStr = oss5.str();

            // create filter and update
            filter_builder.append(kvp("_id", docID));
            update_builder.append(kvp("$set",
                make_document(
                    kvp("truerange", trStr),
                    kvp("avetruerange", atrStr),
                    kvp("supertrendvalue", stStr),
                    kvp("stup", stUpStr),
                    kvp("stdown", stDownStr),
                    kvp("stdirection", kline.STDirection),
                    kvp("action", kline.Action)
                )
            ));
            mongocxx::model::update_one upsert_op(filter_builder.view(), update_builder.view());
            upsert_op.upsert(true);

            bulk.append(upsert_op);
        }
        auto result = bulk.execute();

        if (!result) {
            std::cout << "create_bulk_write failed!!!\n" << std::endl;
        }
    }
    catch (const mongocxx::exception& e) {
        std::cout << "BulkWriteByIds, An exception occurred: " << e.what() << std::endl;
    }
    catch (const std::runtime_error& e) {
        std::cerr << "runtime error: " << e.what() << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "error exception: " << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "unknown error!" << std::endl;
    }
}

std::string MongoManager::SetSettlementItems(std::string dbName, std::string colName, SettlementItem& data) {
    // locate the coll
    auto client = this->mongoPool.acquire();
    auto db = (*client)[dbName.c_str()];
    auto col = db[colName.c_str()];
    std::ostringstream ss;
    ss << "SetSettlementItems: " + colName + "\n" << std::endl;
    std::cout << ss.str();

    std::string symbol = data.Symbol;
    std::string interval = data.Interval;

    std::ostringstream oss1, oss2, oss3, oss4, oss5;
    oss1 << std::fixed << std::setprecision(6) << data.ExecPrice;
    oss2 << std::fixed << std::setprecision(6) << data.ExecVolume;
    oss3 << std::fixed << std::setprecision(6) << data.ProfitValue;
    oss4 << std::fixed << std::setprecision(6) << data.SumValue;
    oss5 << std::fixed << std::setprecision(6) << data.SumAmout;
    std::string ExecPriceStr = oss1.str();
    std::string ExecVolumeStr = oss2.str();
    std::string ProfitValueStr = oss3.str();
    std::string SumValueStr = oss4.str();
    std::string SumAmoutStr = oss5.str();

    auto docValueBuilder = bsoncxx::builder::basic::document{};
    docValueBuilder.append(
        kvp("start_time", data.StartTime),
        kvp("end_time", data.EndTime),
        kvp("symbol", symbol),
        kvp("interval", interval),
        kvp("action", data.Action),
        kvp("tradeID", data.TradeID),
        kvp("execTime", data.ExecTime),
        kvp("exec_price", ExecPriceStr),
        kvp("exec_volume", ExecVolumeStr),
        kvp("profit_value", ProfitValueStr),
        kvp("sum_value", SumValueStr),
        kvp("sum_amout", SumAmoutStr));

    if (!data.PreviousId.empty()) {
        bsoncxx::oid prevID(data.PreviousId);
        docValueBuilder.append(kvp("prevId", prevID));
    }
    bsoncxx::document::value InsertedDoc = docValueBuilder.extract();

    // We choose to move in our document here, which transfers ownership to insert_one()
    try {
        // std::cout << "ready to insert_one: " + colName + "\n" << std::endl;
        auto res = col.insert_one(std::move(InsertedDoc));

        if (!res) {
            std::cout << "Unacknowledged write. No id available." << std::endl;
            return "";
        }

        if (res->inserted_id().type() == bsoncxx::type::k_oid) {
            bsoncxx::oid id = res->inserted_id().get_oid().value;
            std::string id_str = id.to_string();
            std::ostringstream ss;
            ss << "Inserted id: " << id_str << "\n" << std::endl;
            std::cout << ss.str();
            return id_str;
        }
        else {
            std::cout << "Inserted id was not an OID type \n" << std::endl;
            return "";
        }
    }
    catch (const mongocxx::exception& e) {
        std::cout << "An exception occurred: " << e.what() << std::endl;
        return "";
    }
}

void MongoManager::WatchKlineUpdate(std::string dbName, std::string colName, std::vector<Kline>& PreviousTwoKlines) {
    try {
        auto client = this->mongoPool.acquire();
        auto col = (*client)[dbName.c_str()][colName.c_str()];

        mongocxx::options::change_stream options;
        const std::chrono::milliseconds await_time{ 1000 };
        options.max_await_time(await_time);

        mongocxx::change_stream stream = col.watch(options);

        int64_t FinishedStartTime = 0;
        while (true) {
            for (const auto& docEvent : stream) {
                auto klineContent = docEvent["kline"];
                int64_t currentStartTime = klineContent["starttime"].get_int64().value;
                if (FinishedStartTime == 0) {
                    FinishedStartTime = currentStartTime;
                }
                else {
                    if (currentStartTime > FinishedStartTime) {
                        // FinishedStartTime finished updated
                        // get the binding kline

                        // sort decline to fetch the latest 2 fininished klines;
                        mongocxx::options::find opts;
                        opts.sort(make_document(kvp("kline.starttime", -1)));
                        opts.limit(2);
                        auto cursor = col.find(make_document(kvp("kline.starttime", make_document(kvp("$lt", currentStartTime)))), opts);

                        for (auto&& doc : cursor) {
                            Kline klineInst;
                            try {
                                auto existed = this->ParseKline(doc, klineInst);
                                if (existed) {
                                    PreviousTwoKlines.push_back(klineInst);
                                }
                            }
                            catch (const mongocxx::exception& e) {
                                std::cerr << "mongocxx error exception: " << e.what() << std::endl;
                            }
                        }
                        std::reverse(PreviousTwoKlines.begin(), PreviousTwoKlines.end());
                    }
                }
            }
            if (PreviousTwoKlines.size() != 0) {
                return;
            }
            std::cout << dbName + "--" + colName + ": No new notifications. Trying again..." << std::endl;
        }
    }
    catch (const mongocxx::exception& e) {
        std::cout << "WatchKlineUpdate, An exception occurred: " << e.what() << std::endl;
    }
    catch (const std::runtime_error& e) {
        std::cerr << "WatchKlineUpdate runtime error: " << e.what() << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "WatchKlineUpdate error exception: " << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "WatchKlineUpdate unknown error!" << std::endl;
    }
}

void MongoManager::GetKlineUpdate(std::string dbName, std::string colName, std::vector<Kline>& PreviousTwoKlines) {
    try {
        auto client = this->mongoPool.acquire();
        auto col = (*client)[dbName.c_str()][colName.c_str()];

        auto syncedStartTime = this->GetSynedFlag("marketSyncFlag", colName);
        while (true) {
            auto currentSyncedStartTime = this->GetSynedFlag("marketSyncFlag", colName);
            if (syncedStartTime < currentSyncedStartTime) {
                std::vector<Kline> latestKlines;
                this->GetLatestSyncedKlines(currentSyncedStartTime, 2, dbName, colName, latestKlines);
                if (latestKlines.size() != 2) {
                    syncedStartTime = currentSyncedStartTime;
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    continue;
                }
                PreviousTwoKlines.insert(PreviousTwoKlines.end(), latestKlines.begin(), latestKlines.end());
                std::ostringstream ss;
                ss << colName + " PreviousTwoKlines size is: " << PreviousTwoKlines.size() << " currentSyncedTime is: "<< currentSyncedStartTime << "\n" << std::endl;
                std::cout << ss.str();

                return;
            }
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::cout << colName + ": GetKlineUpdate, 5 seconds after, continue... \n" << std::endl;
        }
    }
    catch (const mongocxx::exception& e) {
        std::cerr << "GetKlineUpdate, An exception occurred: " << e.what() << std::endl;
    }
    catch (const std::runtime_error& e) {
        std::cerr << "GetKlineUpdate runtime error: " << e.what() << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "GetKlineUpdate error exception: " << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "GetKlineUpdate unknown error!" << std::endl;
    }
}