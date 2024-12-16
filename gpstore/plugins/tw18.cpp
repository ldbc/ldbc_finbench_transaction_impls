#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw18(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto account_id= args[0];
    Node account_node("Account", "id", &account_id, kvstore, txn); // @todo transform "Account" to account_label_id
    if (account_node.node_id_ == -1)
        return;
    auto isBlocked_prop_id = kvstore->getpropIDBypropStr("isBlocked");
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(account_node.node_id_, isBlocked_prop_id, &value_true, txn);
}