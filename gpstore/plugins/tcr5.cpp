#include "../PQuery/PProcedure.h"
using namespace std;
extern "C" void tcr5(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node person("Person", "id", &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;
    long long startTime = args[1].data_.Long, endTime = args[2].data_.Long;
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

    // if((args[0].data_.Long) == 8796093065209)
    // {
    //     cout<<"gethere\n";
    //     for(int i = 0;i<num_valid_nbr_own; i++)
    //     {
    //         int acc_id = person_own_accounts.dst_ids[i];
    //         Node start_acc(acc_id, kvstore, txn);
    //         cout<<start_acc["id"]->data_.Long<<endl;
    //     }
    //     exit(0);
    // }
    vector<PathSameSrc> whole_paths;
    for(int i = 0;i<num_valid_nbr_own; i++)
    {
        PathSameSrc paths;
        paths.start_id = person_own_accounts.dst_ids[i];
        Node start_acc(paths.start_id, kvstore, txn);
        EdgeInfoList l;
        start_acc.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.edge_props, l.edge_prop_num, l.dst_num, Util::EDGE_OUT);
        int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
        
        // if(start_acc["id"]->data_.Long == 4719491759141290012)
        // {
        //     cout<<"gethere\n";
        //     for(int j = 0;j<num_valid_nbr; j++)
        //     {
        //         int acc_id = l.dst_ids[j];
        //         Node next_acc(acc_id, kvstore, txn);
        //         cout<<next_acc["id"]->data_.Long<<endl;
        //         if(j == 1)
        //         {
        //             cout<<"handling"<<endl;
        //             long long edgeTime = l.edge_props[j * l.edge_prop_num + 1];
        //             cout<<endTime<<' '<<startTime<<' '<<edgeTime<<endl;
        //             // if(edgeTime >= endTime || edgeTime <= startTime) continue;
        //             exit(0);
        //         }
        //     }
        //     exit(0);
        // }
        
        for(int j = 0;j<num_valid_nbr; j++)
        {
            double edgeTime = l.edge_props[j * l.edge_prop_num + 1];
            if(edgeTime >= endTime || edgeTime <= startTime) continue;

            // Node node(l.dst_ids[j],kvstore,txn);
            // cout<<"len1: "<<start_acc["id"]->data_.Long<<' '<<node["id"]->data_.Long<<' '<<edgeTime<<endl;

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
            mid_acc.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.edge_props, l.edge_prop_num, l.dst_num, Util::EDGE_OUT);
            int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
            for(int j = 0;j<num_valid_nbr; j++)
            {
                double edgeTime = l.edge_props[j * l.edge_prop_num + 1];
                if(edgeTime >= endTime || edgeTime <= startTime) continue;
                if(mid_create_time >= edgeTime) continue;

                // Node node(l.dst_ids[j],kvstore,txn);
                // cout<<"len2: "<<mid_acc["id"]->data_.Long<<' '<<node["id"]->data_.Long<<' '<<edgeTime<<endl;

                if(!paths.len2path.count({path_mid, l.dst_ids[j]}))
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
        for(const auto& [path_mid, mid_create_time]: paths.len2path)
        {
            Node mid_acc(path_mid.second, kvstore, txn);
            EdgeInfoList l;
            mid_acc.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", l.dst_ids, l.edge_props, l.edge_prop_num, l.dst_num, Util::EDGE_OUT);
            int num_valid_nbr = Truncate(l, truncationOrder, truncationLimit);
            for(int j = 0;j<num_valid_nbr; j++)
            {
                double edgeTime = l.edge_props[j * l.edge_prop_num + 1];
                if(edgeTime >= endTime || edgeTime <= startTime) continue;
                if(mid_create_time >= edgeTime) continue;

                // Node node(l.dst_ids[j],kvstore,txn);
                // cout<<"mid_create_time: "<<mid_create_time<<endl;
                // cout<<"len3: "<<mid_acc["id"]->data_.Long<<' '<<node["id"]->data_.Long<<' '<<edgeTime<<endl;

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
        auto path_list = new GPStore::Value(GPStore::Value::LIST);
        for(auto& i:path) {
            Node pathNode(i, kvstore, txn);
            path_list->data_.List->emplace_back(new GPStore::Value(pathNode["id"]->data_.Long));
        }
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.emplace_back(*path_list);
    }
}