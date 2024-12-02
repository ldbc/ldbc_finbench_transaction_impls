#include "../PQuery/PProcedure.h"
using namespace std;
struct CompareTcr4Tuples {
    bool operator()(const tuple<int, long long, double, double, long long, double,double>& a, const tuple<int, long long, double, double, long long, double,double>& b) {
        // Compare based on the first three integers in ascending order
        if (get<2>(a) != get<2>(b)) return get<2>(a) > get<2>(b);
        if (get<5>(a) != get<5>(b)) return get<5>(a) > get<5>(b);


        return get<0>(a) < get<0>(b);
    }
};
extern "C" void tcr4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src_account("Account", "id", &args[0], kvstore, txn);
    Node dst_account("Account", "id", &args[1], kvstore, txn);
    if (src_account.node_id_ == -1 || dst_account.node_id_ == -1)
        return;
    int dst_id = dst_account.node_id_;
    double startTime = args[2].data_.Long, endTime = args[3].data_.Long;

    TYPE_ENTITY_LITERAL_ID* neighborList_src = nullptr; unsigned neighbor_len_src = 0;
    long long* prop_list_src = nullptr; unsigned prop_len_src = 0;
    src_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList_src, prop_list_src, prop_len_src, neighbor_len_src, Util::EDGE_OUT);
    bool valid = false;
    for(int i = 0;i<neighbor_len_src;i++){
        double edgeTime = prop_list_src[i * prop_len_src + 1];
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        if(neighborList_src[i] == dst_id) valid = true;
    }
    if(!valid)
    {
        cout<<"empty result"<<endl;
        return;
    }

    TYPE_ENTITY_LITERAL_ID* neighborList_dst = nullptr; unsigned neighbor_len_dst = 0;
    long long* prop_list_dst = nullptr; unsigned prop_len_dst = 0;
    dst_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList_dst, prop_list_dst, prop_len_dst, neighbor_len_dst, Util::EDGE_OUT);
    unordered_map<int,tuple<long long,double, double>> other_id_dst;  /// in tuple: int: num_edges; 2 doubles: sum of account & max of account
    for(int i = 0;i<neighbor_len_dst;i++)
    {
        double edgeTime = prop_list_dst[i * prop_len_dst + 1];
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        int other_id = neighborList_dst[i];
        if(!other_id_dst.count(other_id))
        {
            other_id_dst.insert({other_id,{0,0,0}});
        }
        get<0>(other_id_dst[other_id])++;
        get<1>(other_id_dst[other_id]) += LL2double(prop_list_dst[i * prop_len_dst]);
        get<2>(other_id_dst[other_id]) = max(LL2double(prop_list_dst[i * prop_len_dst]),get<2>(other_id_dst[other_id]));
        
    }


    delete[] prop_list_src;
    prop_list_src = nullptr;
    delete[] neighborList_src;
    neighborList_src = nullptr;
    neighbor_len_src = prop_len_src = 0;
    
    unordered_map<int,tuple<long long , double, double>> other_id_src;  /// in tuple: int: num_edges; 2 doubles: sum of account & max of account
    src_account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList_src, prop_list_src, prop_len_src, neighbor_len_src, Util::EDGE_IN);
    for(int i = 0;i<neighbor_len_src;i++)
    {
        double edgeTime = prop_list_src[i * prop_len_src + 1];
        if(edgeTime >= endTime || edgeTime <= startTime) continue;
        int other_id = neighborList_src[i];
        if(other_id_dst.count(other_id))
        {
            if(!other_id_src.count(other_id))
            {
                other_id_src.insert({other_id,{0,0,0}});
            }
            get<0>(other_id_src[other_id])++;
            get<1>(other_id_src[other_id]) += LL2double(prop_list_src[i * prop_len_src]);
            get<2>(other_id_src[other_id]) = max(LL2double(prop_list_src[i * prop_len_src]),get<2>(other_id_src[other_id]));
        
        }
    }
    vector<tuple<int, long long, double, double, long long, double,double>> ans;
    for(auto& [other_id, info2]: other_id_src)
    {
        long long num_edge2 = get<0>(info2);
        double sum_edge2amount = get<1>(info2);
        double max_edge2amount = get<2>(info2);
        auto& info3 = other_id_dst[other_id];
        long long num_edge3 = get<0>(info3);
        double sum_edge3amount = get<1>(info3);
        double max_edge3amount = get<2>(info3);
        ans.emplace_back(other_id, num_edge2,sum_edge2amount, max_edge2amount, num_edge3, sum_edge3amount, max_edge3amount);

    }
    
    sort(ans.begin(), ans.end(), CompareTcr4Tuples());

    new_result->rows_.reserve(ans.size());
    for(auto&t:  ans)
    {
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(7);
        new_result->rows_.back().values_.emplace_back(get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        new_result->rows_.back().values_.emplace_back(get<3>(t));
        new_result->rows_.back().values_.emplace_back(get<4>(t));
        new_result->rows_.back().values_.emplace_back(get<5>(t));
        new_result->rows_.back().values_.emplace_back(get<6>(t));
        
        cout << get<0>(t) << " " << get<1>(t) << " " << get<2>(t) << " " << get<3>(t) << get<4>(t) << " " << get<5>(t) << " " << get<6>(t) <<endl;
    }
    
}