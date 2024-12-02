#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node mid_account("Account", "id", &args[0], kvstore, txn);
    if (mid_account.node_id_ == -1)
        return;
    double threshold = args[1].data_.Float;
    double startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    int truncationLimit = args[4].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[5].data_.Int);

    bool asc = true; int prop_idx = -1;
    
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    mid_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
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
    int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);
        
    int cntsrc=0, cntdst=0;
    double amountsrc=0, amountdst =0;
    for (unsigned i = 0; i < numValidNeighbor; ++i) 
    {
        // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if(amount <= threshold) continue;
        
        unsigned neighbor_id = neighborList[i];
        cntdst++;
        amountdst += amount;
        
    }

    delete[] neighborList;
    neighborList = nullptr;
    neighbor_len = 0;
    delete[] prop_list;
    prop_list = nullptr;
    prop_len = 0;
    mid_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING)
    {
        asc = true;
        prop_idx = 1;
    }
    else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING)
    {
        asc = false;
        prop_idx = 1;
    }
    else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING)
    {
        asc = true;
        prop_idx = 0;
    }
    else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING)
    {
        asc = false;
        prop_idx = 0;
    }
    numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, prop_idx, truncationLimit, asc);

    for (unsigned i = 0; i < numValidNeighbor; ++i) 
    {
        // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if(amount <= threshold) continue;
        
        unsigned neighbor_id = neighborList[i];
        cntsrc++;
        amountsrc += amount;
        
    }
    float ratio;
    if(cntdst == 0) ratio = -1;
    else ratio = amountsrc/amountdst;

    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(1);
    new_result->rows_.back().values_.emplace_back(cntsrc); 
    new_result->rows_.back().values_.emplace_back(cntdst);
    new_result->rows_.back().values_.emplace_back(ratio);
    cout << cntsrc<<' '<<cntdst<<' '<<ratio << endl;

}