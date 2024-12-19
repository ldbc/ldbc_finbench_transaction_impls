#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw12(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& account_id1 = args[0];
    auto& account_id2 = args[1];
    auto& time = args[2];
    auto& amount = args[3];

    Node account_node1("Account", "id", &account_id1, kvstore, txn);
    Node account_node2("Account", "id", &account_id2, kvstore, txn);
    assert(account_node1.node_id_ != -1);
    assert(account_node2.node_id_ != -1);

    long long ratio_ll;
    memcpy(&ratio_ll, &(amount.data_.Float), sizeof(long long));

    db->AddEdge(account_node1.node_id_, account_node2.node_id_, kvstore->getIDByPredicate("ACCOUNT_TRANSFER_ACCOUNT"),
                {ratio_ll, time.data_.Long, 0}, txn);

}