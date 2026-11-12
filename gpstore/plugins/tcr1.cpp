#include "../PQuery/PProcedure.h"
using namespace std;

struct CompareTuplesTCR1 {
    bool operator()(const tuple<long long, int, long long, string>& a,
                    const tuple<long long, int, long long, string>& b) {
        if (get<1>(a) != get<1>(b)) return get<1>(a) < get<1>(b);
        if (get<0>(a) != get<0>(b)) return get<0>(a) < get<0>(b);
        if (get<2>(a) != get<2>(b)) return get<2>(a) < get<2>(b);
        return get<3>(a) < get<3>(b);
    }
};

namespace {
struct TCR1State {
    long long node_id;
    int distance;
    double last_time;
};

void AddBlockedMediumResults(
        const KVstore *kvstore,
        std::shared_ptr<Transaction> txn,
        long long account_id,
        int distance,
        double startTime,
        double endTime,
        vector<tuple<long long, int, long long, string>> &results,
        set<tuple<long long, int, long long, string>> &unique_results) {
    TYPE_ENTITY_LITERAL_ID* medium_list = nullptr;
    unsigned medium_len = 0;
    long long* prop_list = nullptr;
    unsigned prop_len = 0;

    Node account(account_id, kvstore, txn);
    account.GetLinkedNodesWithEdgeProps(
        "MEDIUM_SIGN_INACCOUNT",
        medium_list,
        prop_list,
        prop_len,
        medium_len,
        Util::EDGE_IN);

    for (unsigned i = 0; i < medium_len; ++i) {
        double sign_in_time = prop_list[i * prop_len];
        if (sign_in_time <= startTime || sign_in_time >= endTime)
            continue;
        Node medium(medium_list[i], kvstore, txn);
        if (!medium["isBlocked"]->data_.Boolean)
            continue;
        tuple<long long, int, long long, string> t = make_tuple(
            account["id"]->data_.Long,
            distance,
            medium["id"]->data_.Long,
            *(medium["mediumType"]->data_.String));
        if (unique_results.insert(t).second)
            results.push_back(t);
    }

    delete[] medium_list;
    delete[] prop_list;
}
}

extern "C" void tcr1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node accountNode("Account", "id", &args[0], kvstore, txn);
    if (accountNode.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);

    vector<tuple<long long, int, long long, string>> results;
    set<tuple<long long, int, long long, string>> unique_results;
    unordered_map<long long, unordered_map<int, double>> best_last_time;
    queue<TCR1State> frontier;
    frontier.push({accountNode.node_id_, 0, -1});
    best_last_time[accountNode.node_id_][0] = -1;

    bool asc = true;
    int prop_idx = -1;
    if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
        asc = true;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
        asc = false;
        prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
        asc = true;
        prop_idx = 0;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
        asc = false;
        prop_idx = 0;
    }

    while (!frontier.empty()) {
        TCR1State cur = frontier.front();
        frontier.pop();
        if (cur.distance == 3)
            continue;

        Node curNode(cur.node_id, kvstore, txn);
        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr;
        unsigned neighbor_len = 0;
        long long* prop_list = nullptr;
        unsigned prop_len = 0;
        curNode.GetLinkedNodesWithEdgeProps(
            "ACCOUNT_TRANSFER_ACCOUNT",
            neighborList,
            prop_list,
            prop_len,
            neighbor_len,
            Util::EDGE_OUT);

        int numValidNeighbor = truncationReorder(
            neighborList,
            neighbor_len,
            prop_list,
            prop_len,
            prop_idx,
            truncationLimit,
            asc);

        for (unsigned i = 0; i < numValidNeighbor; ++i) {
            double edgeTime = prop_list[i * prop_len + 1];
            if (edgeTime >= endTime || edgeTime <= startTime || edgeTime <= cur.last_time)
                continue;

            long long neighbor_id = neighborList[i];
            int next_distance = cur.distance + 1;
            AddBlockedMediumResults(
                kvstore,
                txn,
                neighbor_id,
                next_distance,
                startTime,
                endTime,
                results,
                unique_results);

            auto &distance_to_time = best_last_time[neighbor_id];
            auto it = distance_to_time.find(next_distance);
            if (it != distance_to_time.end() && it->second <= edgeTime)
                continue;
            distance_to_time[next_distance] = edgeTime;
            frontier.push({neighbor_id, next_distance, edgeTime});
        }
        delete[] neighborList;
        delete[] prop_list;
    }

    sort(results.begin(), results.end(), CompareTuplesTCR1());
    new_result->rows_.reserve(results.size());
    for (const auto& t : results) {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(4);
        new_result->rows_.back().values_.emplace_back(get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        new_result->rows_.back().values_.emplace_back(get<3>(t));
    }
}
