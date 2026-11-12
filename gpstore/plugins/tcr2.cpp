#include "../PQuery/PProcedure.h"
using namespace std;

namespace {
struct TCR2State {
    long long node_id;
    int depth;
    double downstream_time;
};

void AddDepositedLoans(
        const KVstore *kvstore,
        std::shared_ptr<Transaction> txn,
        long long account_id,
        double startTime,
        double endTime,
        unordered_map<long long, pair<double, double>> &ans) {
    Node account(account_id, kvstore, txn);
    TYPE_ENTITY_LITERAL_ID* loan_list = nullptr;
    unsigned loan_len = 0;
    long long* prop_list = nullptr;
    unsigned prop_len = 0;
    account.GetLinkedNodesWithEdgeProps(
        "LOAN_DEPOSIT_ACCOUNT",
        loan_list,
        prop_list,
        prop_len,
        loan_len,
        Util::EDGE_IN);

    unordered_set<long long> seen_loans;
    for (unsigned i = 0; i < loan_len; ++i) {
        double deposit_time = prop_list[i * prop_len + 1];
        if (deposit_time <= startTime || deposit_time >= endTime)
            continue;
        if (!seen_loans.insert(loan_list[i]).second)
            continue;
        Node loan(loan_list[i], kvstore, txn);
        ans[account_id].first += loan["loanAmount"]->data_.Float;
        ans[account_id].second += loan["balance"]->data_.Float;
    }

    delete[] loan_list;
    delete[] prop_list;
}

void CollectCR2Candidates(
        const KVstore *kvstore,
        Node &start_account,
        std::shared_ptr<Transaction> txn,
        int truncationLimit,
        TruncationOrderType truncationOrder,
        double startTime,
        double endTime,
        unordered_set<long long> &candidates) {
    queue<TCR2State> frontier;
    frontier.push({start_account.node_id_, 0, endTime});

    unordered_map<long long, unordered_map<int, double>> best_downstream_time;
    best_downstream_time[start_account.node_id_][0] = endTime;

    while (!frontier.empty()) {
        TCR2State cur = frontier.front();
        frontier.pop();
        if (cur.depth == 3)
            continue;

        Node cur_node(cur.node_id, kvstore, txn);
        TYPE_ENTITY_LITERAL_ID* neighbor_list = nullptr;
        unsigned neighbor_len = 0;
        long long* prop_list = nullptr;
        unsigned prop_len = 0;
        cur_node.GetLinkedNodesWithEdgeProps(
            "ACCOUNT_TRANSFER_ACCOUNT",
            neighbor_list,
            prop_list,
            prop_len,
            neighbor_len,
            Util::EDGE_IN);

        int num_valid_neighbor = Truncate(
            neighbor_list,
            neighbor_len,
            prop_list,
            prop_len,
            truncationOrder,
            truncationLimit);

        for (int i = 0; i < num_valid_neighbor; ++i) {
            double edge_time = prop_list[i * prop_len + 1];
            if (edge_time <= startTime || edge_time >= endTime)
                continue;
            if (edge_time >= cur.downstream_time)
                continue;

            long long next_id = neighbor_list[i];
            int next_depth = cur.depth + 1;
            candidates.insert(next_id);

            auto &depth_to_time = best_downstream_time[next_id];
            auto it = depth_to_time.find(next_depth);
            if (it != depth_to_time.end() && it->second >= edge_time)
                continue;
            depth_to_time[next_depth] = edge_time;
            frontier.push({next_id, next_depth, edge_time});
        }

        delete[] neighbor_list;
        delete[] prop_list;
    }
}
}

extern "C" void tcr2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    Node person("Person", "id", &args[0], kvstore, txn);
    if (person.node_id_ == -1)
        return;
    double startTime = args[1].data_.Long, endTime = args[2].data_.Long;
    int truncationLimit = args[3].data_.Int;
    TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[4].data_.Int);
    

    unordered_map<long long, pair<double, double>> ans;  /// nbr id, sum loan amount, sum loan balance
    unordered_set<long long> candidates;


    EdgeInfoList person_own_accounts;
    person.GetLinkedNodes("PERSON_OWN_ACCOUNT", person_own_accounts.dst_ids, person_own_accounts.dst_num, Util::EDGE_OUT);
    int num_valid_nbr_own = Truncate(person_own_accounts, truncationOrder, truncationLimit);

    // TYPE_ENTITY_LITERAL_ID* account_list = nullptr; unsigned account_num = 0;
    // person.GetLinkedNodes("PERSON_OWN_ACCOUNT", account_list, account_num, Util::EDGE_OUT);

    if(num_valid_nbr_own)
    {
        for(int i = 0;i<num_valid_nbr_own;i++)
        {
            Node accountNode(person_own_accounts.dst_ids[i], kvstore, txn); 
            CollectCR2Candidates(
                kvstore,
                accountNode,
                txn,
                truncationLimit,
                truncationOrder,
                startTime,
                endTime,
                candidates);
        }
    }

    for (long long account_id : candidates) {
        AddDepositedLoans(kvstore, txn, account_id, startTime, endTime, ans);
    }
    
    vector<tuple< double,long long, double>> ans_vec;
    for(auto i: ans)
    {
        int internal_id = i.first;
        Node node(internal_id, kvstore,txn);

        ans_vec.emplace_back(- i.second.first, node["id"]->data_.Long, i.second.second);
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
