#include "../PQuery/PProcedure.h"
using namespace std;
struct CompareTuplesTCR8 {
    bool operator()(const tuple<long long, float, int>& a, const tuple<long long, float, int>& b) {
        // Compare based on the first three integers in ascending order
        if (get<2>(a) != get<0>(b)) return get<2>(a) < get<0>(b);
        if (get<1>(a) != get<1>(b)) return get<1>(a) < get<1>(b);
        return get<0>(a) > get<0>(b);
    }
};
extern "C" void tcr8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node loan("Loan", "id", &args[0], kvstore, txn);
    if (loan.node_id_ == -1)
        return;
    float loanAmount = loan["loanAmount"]->data_.Float;
    double threshold = args[1].data_.Float, startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    int truncationLimit = args[4].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[5].data_.Int);
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    loan.GetLinkedNodesWithEdgeProps("LOAN_DEPOSIT_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    if (neighbor_len == 0) {
        delete[] neighborList;
        delete[] prop_list;
        return;
    }
    Node curAccount(neighborList[0], kvstore, txn);
    queue<pair<long long, int>> frontier;
    frontier.push(make_pair(curAccount.node_id_, 0));
    unordered_map<unsigned, double> account2upstream;
    curAccount.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    double curUpstream = 0;
    for (unsigned i = 0; i < neighbor_len; ++i)
        curUpstream += LL2double(prop_list[i * prop_len]);
    account2upstream[curAccount.node_id_] = curUpstream;
    delete[] neighborList;
    delete[] prop_list;
    priority_queue<tuple<long long, float, int>, vector<tuple<long long, float, int>>, CompareTuplesTCR8> pq;
    // Stop at 3 hops
    while (!frontier.empty()) {
        long long curId = frontier.front().first;
        int curDistance = frontier.front().second;
        frontier.pop();
        if (curDistance == 3)
            continue;
        Node curNode(curId, kvstore, txn);

        // Get transfer and withdraw edges, concatenate them for truncation
        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
        long long* prop_list = nullptr; unsigned prop_len = 0;
        curNode.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);   // prop_len == 3
        TYPE_ENTITY_LITERAL_ID* neighborList_other = nullptr; unsigned neighbor_len_other = 0;
        long long* prop_list_other = nullptr; unsigned prop_len_other = 0;
        curNode.GetLinkedNodesWithEdgeProps("ACCOUNT_WITHDRAW_ACCOUNT", neighborList_other, prop_list_other, prop_len_other, neighbor_len_other, Util::EDGE_OUT);   // prop_len_other == 2
        unsigned neighbor_len_total = neighbor_len + neighbor_len_other;
        if (neighbor_len_total == 0) {
            delete[] neighborList;
            delete[] prop_list;
            delete[] neighborList_other;
            delete[] prop_list_other;
            continue;
        }

        TYPE_ENTITY_LITERAL_ID* neighborList_total = new TYPE_ENTITY_LITERAL_ID[neighbor_len_total];
        long long* prop_list_total = new long long[neighbor_len_total * 2];
        memcpy(neighborList_total, neighborList, neighbor_len * sizeof(TYPE_ENTITY_LITERAL_ID));
        memcpy(neighborList_total + neighbor_len, neighborList_other, neighbor_len_other * sizeof(TYPE_ENTITY_LITERAL_ID));
        memcpy(prop_list_total, prop_list, neighbor_len * prop_len * sizeof(long long));
        size_t offset = neighbor_len * 2;
        for (size_t i = 0; i < neighbor_len_other; ++i) {
            prop_list_total[neighbor_len * 2 + i * 2] = prop_list_other[i * 3];
            prop_list_total[neighbor_len * 2 + i * 2 + 1] = prop_list_other[i * 3 + 1];
        }
        delete [] neighborList;
        delete [] prop_list;
        delete [] neighborList_other;
        delete [] prop_list_other;

        prop_len = 2;
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
        int numValidNeighbor = truncationReorder(neighborList_total, neighbor_len_total, prop_list_total, 2, prop_idx, truncationLimit, asc);

        // Get current node's upstream amount
        auto it = account2upstream.find(curId);
        double curUpstream = it->second;
        if (curUpstream < 0)
            continue;
        it->second = -1;    // Mark visited
        double thresholdAmount = curUpstream * threshold;

        for (unsigned i = 0; i < numValidNeighbor; ++i) {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = *((double *)(&prop_list_total[i * prop_len + 1]));
            double edgeAmount = *((double *)(&prop_list_total[i * prop_len]));
            if (edgeTime >= endTime || edgeTime <= startTime || edgeAmount <= thresholdAmount)
                continue;
            unsigned neighbor_id = neighborList_total[i];
            frontier.push(make_pair(neighbor_id, curDistance + 1));
            it = account2upstream.find(neighbor_id);
            double neighborUpstream = 0;
            Node neighborNode(neighbor_id, kvstore, txn);
            if (it == account2upstream.end()) {
                neighborNode.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
                for (unsigned i = 0; i < neighbor_len; ++i)
                    neighborUpstream += *((double *)(prop_list[i * prop_len]));
                account2upstream[neighbor_id] = neighborUpstream;
                delete[] neighborList;
                delete[] prop_list;
            } else
                neighborUpstream = it->second;
            float ratio = float(std::round(1000. * neighborUpstream / loanAmount)) / 1000.;
            pq.push(make_tuple(neighborNode["id"]->data_.Long, ratio, curDistance + 1));
        }
        delete[] neighborList_total;
        delete[] prop_list_total;
    }

    new_result->rows_.reserve(pq.size());
    while (!pq.empty()) {
        auto t = pq.top();
        pq.pop();
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(3);
        new_result->rows_.back().values_.emplace_back(get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t) << endl;
    }
}