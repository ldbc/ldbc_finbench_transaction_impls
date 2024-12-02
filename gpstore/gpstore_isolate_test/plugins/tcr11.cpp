#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src("Person", "id", &args[0], kvstore, txn);
    if (src.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);

    bool asc = true; int prop_idx = -1;
    if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
        asc = true;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
        asc = false;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING)
        assert(false);

    double sumLoanAmount = 0;
    int numLoans = 0;
    std::unordered_set<TYPE_ENTITY_LITERAL_ID> dst_set, src_set{src.node_id_}, visited;
    while (!src_set.empty()) {
        for(auto vid : src_set) {
            auto v = GPStore::Value((int)vid);
            Node cur("Person", "id", &v, kvstore, txn);
            TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
            long long *prop_list = nullptr;  unsigned prop_len = 0;
            cur.GetLinkedNodesWithEdgeProps("PERSON_GUARANTEE_PERSON", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
            int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 0, truncationLimit, asc);

            for(int i = 0; i < numValidNeighbor; ++i) {
                double edge_time = GPStore::Value(prop_list[i * prop_len + 1]).data_.Float;
                if(edge_time > startTime && edge_time < endTime &&
                    visited.find(neighborList[i]) == visited.end()) {
                    visited.insert(neighborList[i]);
                    dst_set.insert(neighborList[i]);
                }
            }
            delete[] neighborList;
            delete[] prop_list;
        }
        swap(src_set, dst_set);
        dst_set.clear();
    }

    for (auto vid : visited) {
        auto v = GPStore::Value((int)vid);
        Node cur("Person", "id", &v, kvstore, txn);
        TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
        long long *prop_list = nullptr;  unsigned prop_len = 0;
        cur.GetLinkedNodesWithEdgeProps("PERSON_APPLY_LOAN", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
        int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 1, truncationLimit, asc);
        numLoans += numValidNeighbor;
        for(int i = 0; i < numValidNeighbor; ++i)
            sumLoanAmount += GPStore::Value(prop_list[i * prop_len]).data_.Float;
        delete []neighborList;
        delete []prop_list;
    }

    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(2);
    new_result->rows_.back().values_.emplace_back(sumLoanAmount);
    new_result->rows_.back().values_.emplace_back(numLoans);
    cout << sumLoanAmount << " " << numLoans << endl;
}