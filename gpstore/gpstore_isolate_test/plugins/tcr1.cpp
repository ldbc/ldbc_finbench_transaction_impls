#include "../PQuery/PProcedure.h"
using namespace std;

struct CompareTuplesTCR1 {
    bool operator()(const tuple<long long, int, long long, string>& a, const tuple<long long, int, long long, string>& b) {
        // Compare based on the first three integers in ascending order
        if (get<0>(a) != get<0>(b)) return get<0>(a) > get<0>(b);
        if (get<1>(a) != get<1>(b)) return get<1>(a) > get<1>(b);
        return get<2>(a) > get<2>(b);
    }
};
extern "C" void tcr1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node accountNode("Account", "id", &args[0], kvstore, txn);
    if (accountNode.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);
    
    priority_queue<tuple<long long, int, long long, string>, vector<tuple<long long, int, long long, string>>, CompareTuplesTCR1> pq;
    unordered_map<long long, double> id2earliest;
    queue<pair<long long, int>> frontier;
    frontier.push(make_pair(accountNode.node_id_, 0));
    id2earliest[accountNode.node_id_] = -1;
    // Stop at 3 hops
    bool asc = true; int prop_idx = -1;
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
        long long curId = frontier.front().first;
        int curDistance = frontier.front().second;
        frontier.pop();
        if (curDistance == 3)
            continue;
        Node curNode(curId, kvstore, txn);
        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
        long long* prop_list = nullptr; unsigned prop_len = 0;
        curNode.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
        int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);
        for (unsigned i = 0; i < numValidNeighbor; ++i) {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = 0;
            memcpy(&edgeTime, &prop_list[i * prop_len + 1], sizeof(double));
            if (edgeTime >= endTime || edgeTime <= startTime || edgeTime <= id2earliest[curId])
                continue;
            unsigned neighbor_id = neighborList[i];
            if (id2earliest.find(neighbor_id) == id2earliest.end()) {
                id2earliest[neighbor_id] = edgeTime;
                frontier.push(make_pair(neighbor_id, curDistance + 1));
                // Check medium here
                TYPE_ENTITY_LITERAL_ID* inner_neighborList = nullptr; unsigned inner_neighbor_len = 0;
                Node neighbor(neighbor_id, kvstore, txn);
                neighbor.GetLinkedNodes("MEDIUM_SIGN_INACCOUNT", inner_neighborList, inner_neighbor_len, Util::EDGE_IN);
                for (unsigned j = 0; j < inner_neighbor_len; ++j) {
                    Node medium(inner_neighborList[j], kvstore, txn);
                    if (medium["isBlocked"]->data_.Boolean) {
                        tuple<long long, int, long long, string> t = make_tuple(neighbor_id, curDistance + 1, medium["id"]->data_.Long, *(medium["mediumType"]->data_.String));
                        pq.push(t);
                        break;
                    }
                }
                delete[] inner_neighborList;
            } else if (edgeTime < id2earliest[neighbor_id]) {
                id2earliest[neighbor_id] = edgeTime;
                frontier.push(make_pair(neighbor_id, curDistance + 1)); // Reexplore the node if the earliest reach time is updated
            }
        }
        delete[] neighborList;
        delete[] prop_list;
    }

    new_result->rows_.reserve(pq.size());
    while (!pq.empty()) {
        auto t = pq.top();
        pq.pop();
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(4);
        new_result->rows_.back().values_.emplace_back(get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        new_result->rows_.back().values_.emplace_back(get<3>(t));
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t) << " " << get<3>(t) << endl;
    }
}