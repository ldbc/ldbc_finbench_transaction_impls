#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node src_account("Account", "id", &args[0], kvstore, txn);
    Node dst_account("Account", "id", &args[1], kvstore, txn);
    if (src_account.node_id_ == -1 || dst_account.node_id_ == -1)
        return;
    int dst_id = dst_account.node_id_;
    double startTime = args[2].data_.Long, endTime = args[3].data_.Long;
    
    
    queue<pair<int,int>> q;
    q.push({src_account.node_id_,0});
    long long ans = -1;
    unordered_set<int>  visited;
    while(!q.empty())
    {
        int path_len = q.front().second;
        Node node (q.front().first,kvstore, txn);
        q.pop();

        TYPE_ENTITY_LITERAL_ID* neighborList = nullptr; unsigned neighbor_len = 0;
        long long* prop_list = nullptr; unsigned prop_len = 0;
        node.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
        for (unsigned i = 0; i < neighbor_len; ++i) 
        {
            // If the edge's timestamp <= the earliest timestamp that can reach the current node, skip this edge
            double edgeTime = prop_list[i * prop_len + 1];
            if(edgeTime >= endTime || edgeTime <= startTime) continue;
            unsigned neighbor_id = neighborList[i];
            if(neighbor_id == dst_id)
            {
                ans = path_len +1;
                break;
            }
            if(!visited.count(neighbor_id))
            {
                visited.insert(neighbor_id);
                q.push({neighbor_id,path_len+1});
            }
        }
        if(ans != -1) break;

    }
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(1);
    new_result->rows_.back().values_.emplace_back(ans);
    
    cout << ans << endl;

}