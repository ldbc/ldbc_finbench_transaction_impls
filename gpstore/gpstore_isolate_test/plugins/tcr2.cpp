#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tcr2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node person("Person", "id", &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);
    

    unordered_map<long long, pair<double, double>> ans;

    TYPE_ENTITY_LITERAL_ID* account_list = nullptr; unsigned account_num = 0;
    person.GetLinkedNodes("PERSON_OWN_ACCOUNT", account_list, account_num, Util::EDGE_OUT);

    if(account_num)
    {
        for(int i = 0;i<account_num;i++)
        {
            Node accountNode(account_list[i], kvstore, txn); 
            BFSWithPathTsOrder(kvstore, accountNode, txn, truncationLimit, truncationOrder, ans,startTime, endTime);
        }
    }
    
    vector<tuple< double,long long, double>> ans_vec;
    for(auto i: ans)
    {
        ans_vec.emplace_back(- i.second.first, i.first, i.second.second);
    }
    sort(ans_vec.begin(), ans_vec.end());


    new_result->rows_.reserve(ans_vec.size());
    for(auto t: ans_vec) {
        
        new_result->rows_.emplace_back();
        new_result->rows_.back().values_.reserve(3);
        new_result->rows_.back().values_.emplace_back(get<1>(t));
        new_result->rows_.back().values_.emplace_back(-get<0>(t));
        new_result->rows_.back().values_.emplace_back(get<2>(t));
        
        cout << get<1>(t) << " " << -get<0>(t) << " " << get<2>(t) << endl;
    }
}