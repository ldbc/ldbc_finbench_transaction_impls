#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node dst_account("Account", "id", &args[0], kvstore, txn);
    if (dst_account.node_id_ == -1)
        return;
    double threshold1 = args[1].data_.Float;
    double threshold2 = args[2].data_.Float;
    double startTime = args[3].data_.Long, endTime = args[4].data_.Long;
    int truncationLimit = args[5].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[6].data_.Int);

    
    
    TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
    long long* prop_list = nullptr; unsigned prop_len = 0;
    dst_account.GetLinkedNodesWithEdgeProps("ACCOUNT_WITHDRAW_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
    int numValidNeighbor = Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
        
    unordered_map<int,double> mid_id_to_edge2amount;
    for (unsigned i = 0; i < numValidNeighbor; ++i) 
    {
        // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
        double edgeTime = LL2double(prop_list[i * prop_len + 1]);
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        double amount = LL2double(prop_list[i * prop_len]);
        if(amount <= threshold2) continue;
        
        unsigned mid_id = neighborList[i];
        if(mid_id_to_edge2amount.count(mid_id))
        {
            mid_id_to_edge2amount.insert({mid_id,0});
        }
        mid_id_to_edge2amount[mid_id]+=amount;
    }

    vector<tuple<double, int, double>> ans; /// -sumedge2amount, midid, sumedge1amount
    for(auto& [mid_id, edge2amount]:mid_id_to_edge2amount)
    {
        delete[] neighborList;
        neighborList = nullptr;
        neighbor_len = 0;
        delete[] prop_list;
        prop_list = nullptr;
        prop_len = 0;
        Node mid_account(mid_id, kvstore, txn);
        mid_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_IN);
        
        int numValidNeighbor =  Truncate(neighborList, neighbor_len, prop_list, prop_len, truncationOrder, truncationLimit);
            
        if(numValidNeighbor<=3)continue;
        int valid_edge1_cnt = 0;
        double amount_sum = 0;
        for (unsigned i = 0; i < numValidNeighbor; ++i) 
        {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = LL2double(prop_list[i * prop_len + 1]);
            if(edgeTime >= endTime || edgeTime <= startTime) continue;
            double amount = LL2double(prop_list[i * prop_len]);
            if(amount <= threshold1) continue;
            
            unsigned neighbor_id = neighborList[i];
            valid_edge1_cnt ++;
            amount_sum+=amount;
        }
        if(valid_edge1_cnt >3) 
        {
            ans.emplace_back(-edge2amount, mid_id, amount_sum);
        }
    }
    sort(ans.begin(), ans.end());
    new_result->rows_.reserve(ans.size());
    for(auto&t:  ans)
    {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(3);
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(-get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t)  <<endl;
    }


}