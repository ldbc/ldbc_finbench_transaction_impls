#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src("Person", "id", &args[0], kvstore, txn);
    if (src.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);

    TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
    long long *prop_list = nullptr;  unsigned prop_len = 0;
    src.GetLinkedNodesWithEdgeProps("PERSON_OWN_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    unordered_map<int, double> compAccount2amount;
    for (unsigned i = 0; i < neighbor_len; i++) {
        Node account(neighborList[i], kvstore, txn);
        TYPE_ENTITY_LITERAL_ID *neighborList2 = nullptr;  unsigned neighbor_len2 = 0;
        long long *prop_list2 = nullptr;  unsigned prop_len2 = 0;
        account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList2, prop_list2, prop_len2, neighbor_len2, Util::EDGE_OUT);
        int numValidNeighbor = Truncate(neighborList2, neighbor_len2, prop_list2, prop_len2, truncationOrder, truncationLimit);
        for (int j = 0; j < numValidNeighbor; j++) {
            double edgeTime = LL2double(prop_list2[j * prop_len2 + 1]);
            if (edgeTime >= startTime && edgeTime <= endTime) {
                double amount = LL2double(prop_list2[j * prop_len2]);
                auto it = compAccount2amount.find(neighborList2[j]);
                if (it == compAccount2amount.end())
                    compAccount2amount[neighborList2[j]] = amount;
                else
                    it->second += amount;
            }
        }
    }

    priority_queue<pair<double, long long>, vector<pair<double, long long>>, greater<pair<double, long long>>> pq;
    for (auto &it : compAccount2amount) {
        Node cur(it.first, kvstore, txn);
        pq.push(make_pair(it.second, cur["id"]->data_.Long));
    }
    while (!pq.empty()) {
        auto &p = pq.top();
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(2);
        new_result->rows_.back().values_.emplace_back(p.second);
        new_result->rows_.back().values_.emplace_back(p.first);
        cout << p.second << " " << p.first << endl;
        pq.pop();
    }
}