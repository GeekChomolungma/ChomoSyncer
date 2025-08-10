#include "db/mongoManager.h"
#include <iomanip> // std::setw, std::setfill
#include <sstream>
#include <iostream>
#include <chrono>
#include <thread>
#include <unordered_set>

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

bool MongoManager::ParseKline(const bsoncxx::document::view& doc, Kline& k) {
    // _id -> 12 bytes
    const auto& oid = doc["_id"].get_oid().value.bytes();
    for (size_t i = 0; i < 12; ++i) k.Id[i] = oid[i];

    // time
    k.StartTime = doc["starttime"].get_int64().value;
    k.EndTime = doc["endtime"].get_int64().value;

    // symbol / interval
    std::string sym = std::string(doc["symbol"].get_string().value);
    std::string itv = std::string(doc["interval"].get_string().value);
#ifdef _WIN32
    strcpy_s(k.Symbol, sym.c_str());
    strcpy_s(k.Interval, itv.c_str());
#else
    strcpy(k.Symbol, sym.c_str());
    strcpy(k.Interval, itv.c_str());
#endif

    // to double lambda function
    auto to_double = [&](const char* key) -> double {
        try {
            return std::stod(std::string(doc[key].get_string().value));
        }
        catch (...) {
            return 0.0;
        }
        };

    // OHLCV
    k.Open = to_double("open");
    k.High = to_double("high");
    k.Low = to_double("low");
    k.Close = to_double("close");
    k.Volume = to_double("volume");
    k.QuoteVolume = to_double("quotevolume");

    // trade info
    k.TradeNum = doc["tradenum"].get_int64().value;
    k.IsFinal = doc["isfinal"].get_bool().value;

    // optional fields
    if (auto v = doc["activebuyvolume"]) { k.ActiveBuyVolume = std::stod(std::string(v.get_string().value)); }
    if (auto v = doc["activebuyquotevolume"]) { k.ActiveBuyQuoteVolume = std::stod(std::string(v.get_string().value)); }

    return true;
}

// sortOrder:
// -1 decline
// 1 arise
void MongoManager::GetKline(int64_t startTime, int64_t endTime, int limit, int sortOrder, std::string dbName, std::string colName, std::vector<Kline>& targetKlineList) {
    auto client = this->mongoPool.acquire();
    auto col = (*client)[dbName.c_str()][colName.c_str()];

    mongocxx::options::find opts;
    opts.sort(make_document(kvp("starttime", sortOrder)));
    opts.limit(limit);

    auto cursor_filtered = 
        col.find( 
            make_document(
                // kvp("kline", make_document(kvp("$exists", true))), 
                kvp("starttime", make_document(kvp("$gte", startTime), kvp("$lte", endTime)))
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
    auto db = (*client)[dbName];
    auto col = db[colName];

    mongocxx::options::find opts;
    opts.sort(make_document(kvp("starttime", -1)));
    opts.limit(limit);

    bsoncxx::builder::basic::document filter;
    if (endTime != 0) {
        filter.append(kvp("starttime", make_document(kvp("$lte", bsoncxx::types::b_int64{ endTime }))));
    }
    auto cursor_filtered = col.find(filter.view(), opts);

    for (auto&& doc : cursor_filtered) {
        // std::cout << "GetLatestSyncedKlines, doc: " << bsoncxx::to_json(doc) << std::endl;
        Kline klineInst;
        auto existed = this->ParseKline(doc, klineInst);
        if (existed) {
            fetchedDataPerCol.push_back(klineInst);
        } 
    }
    std::reverse(fetchedDataPerCol.begin(), fetchedDataPerCol.end());

    std::ostringstream ss;
    ss << "GetLatestSyncedKlines, dbName: " << dbName << " colName: " << colName << ", EndTime up to: " << endTime << ", max limit: " << limit << ", fetchedDataPerCol size is: " << fetchedDataPerCol.size() << "\n" << std::endl;
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
            
            auto latest_kline = *cursor_filtered.begin();
            latestSyncedStartTime = latest_kline["starttime"].get_int64();
            latestSyncedEndTime = latest_kline["endtime"].get_int64();
            std::cout << "GetLatestSyncedTime " << colName << " found latest kline, time range: [" 
                << latestSyncedStartTime << ", " << latestSyncedEndTime << "]" << std::endl;
            // auto sync_start_time = latestSyncedEndTime + 1;
        }else{
            latestSyncedStartTime = 0;
            latestSyncedEndTime = 0;
            std::cout << "GetLatestSyncedTime " << colName << " no kline found, set time range to [0, 0]" << std::endl;
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

std::optional<IndicatorState> MongoManager::ReadIndicatorLatestState(const std::string& dbName, const std::string& colName) {
    try {
        auto client = mongoPool.acquire();
        auto col = (*client)[dbName][colName];

        mongocxx::options::find opts;
        // sorted by starttime descending
        bsoncxx::builder::basic::document sort_doc;
        sort_doc.append(bsoncxx::builder::basic::kvp("starttime", -1));
        opts.sort(sort_doc.view());
        opts.limit(1);

        auto cursor = col.find({}, opts);
        for (auto&& doc : cursor) {
            IndicatorState st;

            if (auto el = doc["name"];      el && el.type() == bsoncxx::type::k_utf8)  st.name = std::string(el.get_utf8().value);
            if (auto el = doc["symbol"];      el && el.type() == bsoncxx::type::k_utf8)  st.symbol = std::string(el.get_utf8().value);
            if (auto el = doc["interval"];      el && el.type() == bsoncxx::type::k_utf8)  st.interval = std::string(el.get_utf8().value);
            
            if (auto el = doc["starttime"]; el && el.type() == bsoncxx::type::k_int64)  st.startTime = el.get_int64().value;
            if (auto el = doc["endtime"];   el && el.type() == bsoncxx::type::k_int64)  st.endTime = el.get_int64().value;
            if (auto el = doc["period"];    el) { double v; if (element_to_double(el, v)) st.period = static_cast<int>(v); }

            // put the dynamic fields to vector
            for (auto&& el : doc) {
                std::string key = std::string(el.key());
                if (is_fixed_field(key)) continue;
                double v;
                if (element_to_double(el, v)) {
                    st.values.emplace(std::move(key), v);
                }
            }
            return st;
        }
        return std::nullopt; 
    }
    catch (const std::exception& e) {
        std::cerr << "ReadIndicatorLatest error: " << e.what() << std::endl;
        return std::nullopt;
    }
}

void MongoManager::WriteIndicatorState(std::string dbName, std::string colName, const bsoncxx::document::view& doc) {
    try {
        auto client = mongoPool.acquire();
        auto col = (*client)[dbName][colName];

        // primary key: starttime
        bsoncxx::builder::basic::document filter;
        filter.append(kvp("starttime", doc["starttime"].get_int64().value));

        // $set
        bsoncxx::builder::basic::document update;
        update.append(kvp("$set", doc));

        mongocxx::options::update opts;
        opts.upsert(true);
        col.update_one(filter.view(), update.view(), opts);

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

void MongoManager::BulkWriteClosedKlines(std::string dbName,
    std::string colName,
    std::vector<KlineResponseWs>& rawData) 
{
    if (rawData.empty()) return;

    try {
        auto client = mongoPool.acquire();
        auto col = (*client)[dbName][colName];

        // unordered bulk faster, as it wont block on errors
        mongocxx::options::bulk_write bw_opts;
        bw_opts.ordered(false);
        auto bulk = col.create_bulk_write(bw_opts);

        std::vector<int64_t> opStartTimes;
        opStartTimes.reserve(rawData.size());

        for (auto& kline : rawData) {
            // starttime as main key filter
            auto filter = bsoncxx::builder::basic::make_document(
                kvp("starttime", bsoncxx::types::b_int64{ kline.StartTime })
            );

            // entire kline document
            bsoncxx::builder::basic::document doc;
            doc.append(
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

            // $set + upsert
            auto update = bsoncxx::builder::basic::make_document(
                kvp("$set", doc.view())
            );
            mongocxx::model::update_one op{ filter.view(), update.view() };
            op.upsert(true);
            bulk.append(op);

            // or replace whole the entry£ºreplace_one + upsert
            // mongocxx::model::replace_one op{ filter.view(), doc.view() };
            // op.upsert(true);
            // bulk.append(op);

            opStartTimes.push_back(kline.StartTime);
        }

        auto res = bulk.execute();
        if (!res) {
            std::cerr << "Bulk upsert failed\n";
        }

        // statistics for the bulk operation
        const auto inserted_map = res->upserted_ids();   // inserted ids map
        const auto inserted_cnt = inserted_map.size();
        const auto matched_cnt = res->matched_count();  // the existing entries matched by the filter
        const auto modified_cnt = res->modified_count(); // the entries modified by the update

        // reflect the inserted start times
        std::vector<int64_t> inserted_starts;
        inserted_starts.reserve(inserted_cnt);
        for (auto&& kv : inserted_map) {
            size_t op_idx = kv.first; // keep the order of insertion
            if (op_idx < opStartTimes.size())
                inserted_starts.push_back(opStartTimes[op_idx]);
        }

        std::cout << "[BulkUpsert] " << dbName << "." << colName
            << " total_ops=" << rawData.size()
            << " inserted=" << inserted_cnt
            << " matched=" << matched_cnt
            << " modified=" << modified_cnt
            << std::endl;

        if (!inserted_starts.empty()) {
            std::cout << "  inserted starttimes (sample up to 10): [";
            for (size_t i = 0; i < inserted_starts.size() && i < 10; ++i) {
                if (i) std::cout << ", ";
                std::cout << inserted_starts[i];
            }
            if (inserted_starts.size() > 10) std::cout << ", ...";
            std::cout << "]\n";
        }
        // it's not necessary to return the inserted ids, but can be useful for debugging
    }
    catch (const std::exception& e) {
        std::cerr << "BulkWriteClosedKlines upsert error: " << e.what() << '\n';
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
                        opts.sort(make_document(kvp("starttime", -1)));
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