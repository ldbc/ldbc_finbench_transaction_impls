/**
 * Copyright 2022 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

#include <algorithm>
#include <exception>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <queue>
#include "lgraph/lgraph.h"
#include "lgraph/lgraph_edge_iterator.h"
#include "lgraph/lgraph_types.h"
#include "lgraph/lgraph_utils.h"
#include "lgraph/lgraph_result.h"
#include "tools/json.hpp"
#include "fma-common/utils.h"

using namespace lgraph_api;
using json = nlohmann::json;

template <class EIT>
class LabeledEdgeIterator : public EIT {
    uint16_t lid_;
    bool valid_;

   public:
    LabeledEdgeIterator(EIT&& eit, uint16_t lid) : EIT(std::move(eit)), lid_(lid) {
        valid_ = EIT::IsValid() && EIT::GetLabelId() == lid_;
    }

    bool IsValid() { return valid_; }

    bool Next() {
        if (!valid_) return false;
        valid_ = (EIT::Next() && EIT::GetLabelId() == lid_);
        return valid_;
    }

    void Reset(VertexIterator& vit, uint16_t lid) { Reset(vit.GetId(), lid); }

    void Reset(size_t vid, uint16_t lid, int64_t tid = 0) {
        lid_ = lid;
        if (std::is_same<EIT, OutEdgeIterator>::value) {
            EIT::Goto(EdgeUid(vid, 0, lid, tid, 0), true);
        } else {
            EIT::Goto(EdgeUid(0, vid, lid, tid, 0), true);
        }
        valid_ = (EIT::IsValid() && EIT::GetLabelId() == lid_);
    }
};

static LabeledEdgeIterator<OutEdgeIterator> LabeledOutEdgeIterator(VertexIterator& vit, uint16_t lid, int64_t tid = 0) {
    return LabeledEdgeIterator<OutEdgeIterator>(std::move(vit.GetOutEdgeIterator(EdgeUid(0, 0, lid, tid, 0), true)),
                                                lid);
}

static LabeledEdgeIterator<InEdgeIterator> LabeledInEdgeIterator(VertexIterator& vit, uint16_t lid, int64_t tid = 0) {
    return LabeledEdgeIterator<InEdgeIterator>(std::move(vit.GetInEdgeIterator(EdgeUid(0, 0, lid, tid, 0), true)), lid);
}

static LabeledEdgeIterator<OutEdgeIterator> LabeledOutEdgeIterator(Transaction& txn, int64_t vid, uint16_t lid,
                                                                   int64_t tid = 0) {
    return LabeledEdgeIterator<OutEdgeIterator>(std::move(txn.GetOutEdgeIterator(EdgeUid(vid, 0, lid, tid, 0), true)),
                                                lid);
}

static LabeledEdgeIterator<InEdgeIterator> LabeledInEdgeIterator(Transaction& txn, int64_t vid, uint16_t lid,
                                                                 int64_t tid = 0) {
    return LabeledEdgeIterator<InEdgeIterator>(std::move(txn.GetInEdgeIterator(EdgeUid(0, vid, lid, tid, 0), true)),
                                               lid);
}

bool compare(std::tuple<int64_t, int64_t, int32_t> t1, std::tuple<int64_t, int64_t, int32_t> t2)
{
  return std::get<0>(t1) > std::get<0>(t2);
}

struct cmp  
{ 
  bool operator()(const std::tuple<int64_t, int32_t, int64_t, std::string>& l, 
                  const std::tuple<int64_t, int32_t, int64_t, std::string>& r) const
  {
    if (std::get<0>(l) == std::get<0>(r) && std::get<1>(l) == std::get<1>(r) && std::get<2>(l) == std::get<2>(r)) {
        return 0;
    }
    return std::get<1>(l) == std::get<1>(r)
                ? (std::get<0>(l) == std::get<0>(r) ? std::get<2>(l) < std::get<2>(r)
                                                    : std::get<0>(l) < std::get<0>(r))
                : std::get<1>(l) < std::get<1>(r);
  } 
}; 

extern "C" bool Process(GraphDB& db, const std::string& request, std::string& response) {
    // start of execution time
    double t0 = fma_common::GetTime();

    static const std::string ACCOUNT_ID = "id";
    static const std::string ACCOUNT_LABEL = "Account";
    static const std::string TRANSFER_LABEL = "transfer";
    static const std::string TIMESTAMP = "timestamp";
    static const std::string SIGNIN_LABEL = "signIn";
    static const std::string MEDIUM_LABEL = "Medium";
    static const std::string MEDIUM_ISBLOCKED = "isBlocked";
    static const std::string MEDIUM_ID = "id";
    static const std::string MEDIUM_TYPE = "type";
    json output;
    int64_t id, start_time, end_time;
    int32_t truncation_limit;
    try {
        json input = json::parse(request);
        parse_from_json(id, "id", input);
        parse_from_json(start_time, "startTime", input);
        parse_from_json(end_time, "endTime", input);
        parse_from_json(truncation_limit, "truncationLimit", input);
    } catch (std::exception& e) {
        output["msg"] = "json parse error: " + std::string(e.what());
        response = output.dump();
        return false;
    }

    auto txn = db.CreateReadTxn();

    // get the vertex iterator of source account
    auto account = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUNT_ID, FieldData(id));
    auto vit = txn.GetVertexIterator();
    int64_t account_vid;
    account_vid = account.GetId();
    vit.Goto(account_vid);

    // get the lid of edge "transfer" and "signIn"
    int16_t transfer_id = (int16_t)txn.GetEdgeLabelId(TRANSFER_LABEL);
    int16_t signIn_id = (int16_t)txn.GetEdgeLabelId(SIGNIN_LABEL);

    // timer for iteration time
    double t_iteration = 0;
    // search all nodes within (1, 3) hop
    std::vector<std::tuple<int64_t, int64_t, int32_t>> others;
    std::queue<std::tuple<int64_t, int64_t, int32_t>> tmp_queue;
    tmp_queue.push(std::make_tuple(account_vid, 0, 0));
    for (int hop = 0; hop < 3; hop++) {
        int length = tmp_queue.size();
        for (int i = 0; i < length; i++) {
            // store and pop current node
            std::tuple<int64_t, int64_t, int32_t> tmp_node = tmp_queue.front();
            tmp_queue.pop();
            others.emplace_back(tmp_node);
            auto old_timestamp = std::get<1>(tmp_node);

            // iterate over all transfer-outs edges within (startTime, endTime), then get vid of end node
            std::vector<std::tuple<int64_t, int64_t, int32_t>> end_nodes;
            vit.Goto(std::get<0>(tmp_node));
            // start of iteration
            double t1 = fma_common::GetTime();
            for (auto eit = LabeledOutEdgeIterator(vit, transfer_id, start_time);
                    eit.IsValid(); eit.Next()) {
                auto timestamp = eit.GetField(TIMESTAMP).AsInt64();
                if (timestamp == start_time) {
                    continue;
                }
                if (timestamp >= end_time) {
                    break;
                }
                if (timestamp > old_timestamp) {
                    end_nodes.emplace_back(std::make_tuple(eit.GetDst(), timestamp, hop + 1));
                }
                // end_nodes.emplace_back(std::make_tuple(eit.GetDst(), timestamp, hop + 1));
            }
            // end of iteration
            double t2 = fma_common::GetTime();
            // add to total iteration time
            t_iteration += t2 - t1;

            // sort all selected nodes in timestamp descending order
            std::sort(end_nodes.begin(), end_nodes.end(),
                    [=](std::tuple<int64_t, int64_t, int32_t>& l, std::tuple<int64_t, int64_t, int32_t>& r) {
                        return std::get<1>(l) > std::get<1>(r);
                    });

            // truncate nodes, then push into queue
            for (int j = 0; j < end_nodes.size() && j < truncation_limit; j++) {
                tmp_queue.push(end_nodes[j]);
            }
        }
    }
    
    while (tmp_queue.size() != 0) {
        others.emplace_back(tmp_queue.front());
        tmp_queue.pop();
    }

    // iterate over all other nodes, chechk if it is signed in by a blocked Medium, if yes, store it to result
    auto midium_vit = txn.GetVertexIterator();
    // std::vector<std::tuple<int64_t, int32_t, int64_t, std::string>> result;
    std::set<std::tuple<int64_t, int32_t, int64_t, std::string>, cmp> result;
    for (int i = 1; i < others.size(); i++) {
        auto other = others[i];
        int64_t other_vid = std::get<0>(other);
        vit.Goto(other_vid);
        for (auto eit = LabeledInEdgeIterator(vit, signIn_id, start_time);
                eit.IsValid(); eit.Next()) {
            auto timestamp = eit.GetField(TIMESTAMP).AsInt64();
            if (timestamp == start_time) {
                continue;
            }
            if (timestamp >= end_time) {
                break;
            }
            midium_vit.Goto(eit.GetSrc());
            bool isBlocked = midium_vit.GetField(MEDIUM_ISBLOCKED).AsBool();
            if (isBlocked) {
                int64_t other_id = vit.GetField(ACCOUNT_ID).AsInt64();
                int32_t dist = std::get<2>(other);
                int64_t medium_id = midium_vit.GetField(MEDIUM_ID).AsInt64();
                std::string medium_type = midium_vit.GetField(MEDIUM_TYPE).AsString();
                result.insert(std::make_tuple(other_id, dist, medium_id, medium_type));
            }
        }
    }

    lgraph_api::Result api_result(
        {{"otherId", LGraphType::INTEGER}, {"accountDistance", LGraphType::INTEGER}, {"mediumId", LGraphType::INTEGER}, {"mediumType", LGraphType::STRING}, {"iterationTime", LGraphType::DOUBLE}, {"executionTime", LGraphType::DOUBLE}});
    for (auto& item : result) {
        auto r = api_result.MutableRecord();
        r->Insert("otherId", FieldData::Int64(std::get<0>(item)));
        r->Insert("accountDistance", FieldData::Int32(std::get<1>(item)));
        r->Insert("mediumId", FieldData::Int64(std::get<2>(item)));
        r->Insert("mediumType", FieldData::String(std::get<3>(item)));
    }
    // end of execution time
    double t3 = fma_common::GetTime();
    auto r = api_result.MutableRecord();
    r->Insert("otherId", FieldData::Int64(0));
    r->Insert("accountDistance", FieldData::Int32(0));
    r->Insert("mediumId", FieldData::Int64(0));
    r->Insert("mediumType", FieldData::String(""));
    r->Insert("iterationTime", FieldData::Double(t_iteration));
    r->Insert("executionTime", FieldData::Double(t3 - t0));
    response = api_result.Dump();
    return true;
}
