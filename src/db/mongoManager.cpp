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

    bsoncxx::stdx::string_view volStrTmp = klineContent["volume"].get_string().value;
    klineInst.Volume = std::stod(std::string(volStrTmp));

    klineInst.TradeNum = klineContent["tradenum"].get_int64();
    klineInst.IsFinal = klineContent["isfinal"].get_bool();

    bsoncxx::stdx::string_view qutovStrTmp = klineContent["quotevolume"].get_string().value;
    klineInst.QuoteVolume = std::stod(std::string(qutovStrTmp));

    return true;
}

// sortOrder:
// -1 decline
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

void MongoManager::GetLatestSyncedTime(std::string dbName, std::string colName, int64_t& latestSyncedStartTime, int64_t& latestSyncedEndTime) {
    try{
        // locate the coll
        auto client = this->mongoPool.acquire();
        auto db = (*client)[dbName];
        auto col = db[colName];
        
        // Check if a document with the same starttime exists
        mongocxx::options::find opts;
        opts.sort(make_document(kvp("starttime", -1)));
        opts.limit(1);
        auto cursor_filtered = col.find({}, opts);

        if (cursor_filtered.begin() != cursor_filtered.end()) {
            std::cout << "GetLatestSyncedTime " << colName << " found latest kline!" << std::endl;
            auto latest_kline = *cursor_filtered.begin();
            latestSyncedStartTime = latest_kline["starttime"].get_int64();
            latestSyncedEndTime = latest_kline["endtime"].get_int64();
            // auto sync_start_time = latestSyncedEndTime + 1;
        }else{
            latestSyncedStartTime = 0;
            latestSyncedEndTime = 0;
        }
    } catch (const mongocxx::exception& e) {
        std::cout << "GetLatestSyncedTime, An exception occurred: " << e.what() << std::endl;
    } catch (const std::runtime_error& e) {
        std::cerr << "GetLatestSyncedTime runtime error: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "GetLatestSyncedTime error exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "GetLatestSyncedTime unknown error!" << std::endl;
    }
}

// Mongo Write Function Implementation
void MongoManager::WriteClosedKlines(std::string dbName, std::vector<KlineResponseWs>& rawData) {
    try {
        if (rawData.empty()) {
            return;
        }
        
        auto client = mongoPool.acquire();

        for (auto& kline : rawData) {
            // Determine the correct database and collection based on the symbol and interval
            std::string colName = std::string(kline.Symbol) + "_" + std::string(kline.Interval) + "_" + "Binance";  // This can be adjusted to derive from kline.Symbol or other properties
            auto db = (*client)[dbName];
            auto col = db[colName];

            // Check if a document with the same starttime exists
            bsoncxx::builder::basic::document filter_builder;
            filter_builder.append(kvp("starttime", kline.StartTime));
            auto existing_doc = col.find_one(filter_builder.view());

            // Build the insert or update document
            bsoncxx::builder::basic::document doc_builder;
            doc_builder.append(
                kvp("eventtype", kline.EventType),
                kvp("eventtime", kline.EventTime),
                kvp("symbol", kline.Symbol),
                kvp("starttime", kline.StartTime),
                kvp("endtime", kline.EndTime),
                kvp("interval", kline.Interval),
                kvp("firsttradeid", kline.FirstTradeID),
                kvp("lasttradeid", kline.LastTradeID),
                kvp("open", kline.Open),
                kvp("high", kline.High),
                kvp("low", kline.Low),
                kvp("close", kline.Close),
                kvp("volume", kline.Volume),
                kvp("tradenum", kline.TradeNum),
                kvp("isfinal", kline.IsFinal),
                kvp("quotevolume", kline.QuoteVolume),
                kvp("activebuyvolume", kline.ActiveBuyVolume),
                kvp("activebuyquotevolume", kline.ActiveBuyQuoteVolume),
                kvp("ignoreparam", kline.IgnoreParam)
            );

            if (existing_doc) {
                // Update the existing document
                bsoncxx::builder::basic::document update_builder;
                update_builder.append(kvp("$set", doc_builder.view()));
                col.update_one(filter_builder.view(), update_builder.view());
            } else {
                // Insert the new document
                auto result = col.insert_one(doc_builder.view());
                if (!result) {
                    std::cerr << "Insert failed for symbol: " << kline.Symbol << "\n";
                }
            }
        }
    }
    catch (const mongocxx::exception& e) {
        std::cout << "WriteClosedKlines, An exception occurred: " << e.what() << std::endl;
    }
    catch (const std::runtime_error& e) {
        std::cerr << "WriteClosedKlines runtime error: " << e.what() << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "WriteClosedKlines error exception: " << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "WriteClosedKlines unknown error!" << std::endl;
    }
}

void MongoManager::WriteIndicator(std::string dbName, std::string colName, const bsoncxx::v_noabi::document::view& doc) {
    try {
        auto client = mongoPool.acquire();
        auto db = (*client)[dbName];
        auto col = db[colName];
        // Check if a document with the same starttime exists
        bsoncxx::builder::basic::document filter_builder;
        filter_builder.append(kvp("starttime", doc["starttime"].get_int64().value));
        auto existing_doc = col.find_one(filter_builder.view());
        if (existing_doc) {
            // Update the existing document
            bsoncxx::builder::basic::document update_builder;
            update_builder.append(kvp("$set", doc));
            col.update_one(filter_builder.view(), update_builder.view());
        } else {
            // Insert the new document
            col.insert_one(doc);
        }
    }
    catch (const mongocxx::exception& e) {
        std::cout << "WriteIndicator, An exception occurred: " << e.what() << std::endl;
    }
    catch (const std::runtime_error& e) {
        std::cerr << "WriteIndicator runtime error: " << e.what() << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "WriteIndicator error exception: " << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "WriteIndicator unknown error!" << std::endl;
    }
}

void MongoManager::BulkWriteClosedKlines(std::string dbName, std::string colName, std::vector<KlineResponseWs>& rawData) {
    try {
        if (rawData.empty()) {
            return;
        }

        auto client = mongoPool.acquire();
        auto db = (*client)[dbName];
        auto col = db[colName];
        auto bulk_write = col.create_bulk_write();

        for (auto& kline : rawData) {
            // Check if a document with the same starttime exists
            bsoncxx::builder::basic::document filter_builder;
            filter_builder.append(kvp("starttime", kline.StartTime));
            auto existing_doc = col.find_one(filter_builder.view());

            // Build the insert or update document
            bsoncxx::builder::basic::document doc_builder;
            doc_builder.append(
                kvp("eventtype", kline.EventType),
                kvp("eventtime", kline.EventTime),
                kvp("symbol", kline.Symbol),
                kvp("starttime", kline.StartTime),
                kvp("endtime", kline.EndTime),
                kvp("interval", kline.Interval),
                kvp("firsttradeid", kline.FirstTradeID),
                kvp("lasttradeid", kline.LastTradeID),
                kvp("open", kline.Open),
                kvp("high", kline.High),
                kvp("low", kline.Low),
                kvp("close", kline.Close),
                kvp("volume", kline.Volume),
                kvp("tradenum", kline.TradeNum),
                kvp("isfinal", kline.IsFinal),
                kvp("quotevolume", kline.QuoteVolume),
                kvp("activebuyvolume", kline.ActiveBuyVolume),
                kvp("activebuyquotevolume", kline.ActiveBuyQuoteVolume),
                kvp("ignoreparam", kline.IgnoreParam)
            );

            if (existing_doc) {
                // Update the existing document
                bsoncxx::builder::basic::document update_builder;
                update_builder.append(kvp("$set", doc_builder.view()));
                bulk_write.append(mongocxx::model::update_one{ filter_builder.view(), update_builder.view() });
            } else {
                // Insert the new document
                bulk_write.append(mongocxx::model::insert_one{ doc_builder.view() });
            }
        }

        auto bulk_result = bulk_write.execute();
        if (!bulk_result) {
            std::cerr << "BulkWriteClosedKlines Bulk insert failed" << std::endl;
        }
    }
    catch (const mongocxx::exception& e) {
        std::cout << "BulkWriteClosedKlines, An exception occurred: " << e.what() << std::endl;
    }
    catch (const std::runtime_error& e) {
        std::cerr << "BulkWriteClosedKlines runtime error: " << e.what() << std::endl;
    }
    catch (const std::exception& e) {
        std::cerr << "BulkWriteClosedKlines error exception: " << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "BulkWriteClosedKlines unknown error!" << std::endl;
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