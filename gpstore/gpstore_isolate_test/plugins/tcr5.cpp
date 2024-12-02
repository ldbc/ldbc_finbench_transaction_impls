#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node person("Person", "id", &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);
    

    
    struct HashPairInt
    {
        ssize_t operator()(pair<int,int> p) const
        {
            long long combined = static_cast<long long>(p.first) | (static_cast<long long>(p.second) << 32);
            // 使用 std::hash<long long> 进行哈希计算
            return std::hash<long long>{}(combined);
        }
        
    };
    struct PathSameSrc
    {
        int start_id = -1;
        unordered_map<int,double> len1path;  /// double: create time
        unordered_map<pair<int,int>,double,HashPairInt> len2path;
        set<tuple<int,int,int>> len3path;
    };
    

    EdgeInfoList person_own_accounts;
    person.GetLinkedNodes("PERSON_OWN_ACCOUNT", person_own_accounts.dst_ids, person_own_accounts.dst_num, Util::EDGE_OUT);
    int num_valid_nbr_own = Truncate(person_own_accounts, truncationOrder, truncationLimit);

    vector<PathSameSrc> whole_paths;
    for(int i = 0;i<num_valid_nbr_own; i++)
    {
        PathSameSrc paths;
        paths.start_id = person_own_accounts.dst_ids[i];
        Node start_acc(paths.start_id, kvstore, txn);
        EdgeInfoList l;
        start_acc.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.edge_props, l.edge_prop_num, l.dst_num, Util::EDGE_OUT);
        int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
        for(int j = 0;j<num_valid_nbr; j++)
        {
            double edgeTime = LL2double(l.edge_props[j * l.edge_prop_num + 1]);
            if(edgeTime >= endTime || edgeTime <= startTime) continue;

            if(!paths.len1path.count(l.dst_ids[j]))
            {
                paths.len1path.insert({l.dst_ids[j],edgeTime});
            }
            else
            {
                if(paths.len1path[l.dst_ids[j]] > edgeTime)
                {
                    paths.len1path[l.dst_ids[j]] = edgeTime;
                }
            }
        }

        for(auto& [path_mid, mid_create_time]: paths.len1path)
        {
            Node mid_acc(path_mid, kvstore, txn);
            EdgeInfoList l;
            mid_acc.GetLinkedNodes("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.dst_num, Util::EDGE_OUT);
            int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
            for(int j = 0;j<num_valid_nbr; j++)
            {
                double edgeTime = LL2double(l.edge_props[j * l.edge_prop_num + 1]);
                if(edgeTime >= endTime || edgeTime <= startTime) continue;
                if(mid_create_time >= edgeTime) continue;

                if(paths.len2path.count({path_mid, l.dst_ids[j]}))
                {
                    paths.len2path.insert({{path_mid, l.dst_ids[j]},edgeTime});
                }
                else
                {
                    if(paths.len2path[{path_mid, l.dst_ids[j]}] > edgeTime)
                    {
                        paths.len2path[{path_mid, l.dst_ids[j]}] = edgeTime;
                    }
                }
            }
        }
        for(auto& [path_mid, mid_create_time]: paths.len2path)
        {
            Node mid_acc(path_mid.second, kvstore, txn);
            EdgeInfoList l;
            mid_acc.GetLinkedNodes("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.dst_num, Util::EDGE_OUT);
            int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
            for(int j = 0;j<num_valid_nbr; j++)
            {
                double edgeTime = LL2double(l.edge_props[j * l.edge_prop_num + 1]);
                if(edgeTime >= endTime || edgeTime <= startTime) continue;
                if(mid_create_time >= edgeTime) continue;

                paths.len3path.insert({path_mid.first, path_mid.second, l.dst_ids[j]});
            }
        }
        whole_paths.emplace_back(paths);

    }


    vector<vector<int>> ans;
    for(int path_len = 3; path_len>=1; path_len--)
    {
        for(auto& path_same_src: whole_paths)
        {
            switch (path_len)
            {
                case 1:
                    for(auto& [len1path,_]: path_same_src.len1path)
                    {
                        ans.emplace_back();
                        ans.back().emplace_back(path_same_src.start_id);
                        ans.back().emplace_back(len1path);
                    }
                    break;
                case 2:
                    for(auto& [len2path,_]: path_same_src.len2path)
                    {
                        ans.emplace_back();
                        ans.back().emplace_back(path_same_src.start_id);
                        ans.back().emplace_back(len2path.first);
                        ans.back().emplace_back(len2path.second);
                    }
                    break;
                case 3:
                    for(auto& [acc1,acc2, acc3]: path_same_src.len3path)
                    {
                        ans.emplace_back();
                        ans.back().emplace_back(path_same_src.start_id);
                        ans.back().emplace_back(acc1);
                        ans.back().emplace_back(acc2);
                        ans.back().emplace_back(acc3);
                    }
                    break;
                default:
                    assert(0);
            }
        }
    }

    new_result->rows_.reserve(ans.size());
    for(auto&path:  ans)
    {
        new_result->rows_.emplace_back();
        for(auto& i:path)
        {
            new_result->rows_.back().values_.emplace_back(i);
            cout<<i<<' ';
        }
        cout<<endl;
        
        
    }


}