#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw19(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto person_id = args[0];
    Node person_node("Person", "id", &person_id, kvstore, txn);
    if (person_node.node_id_ == -1)
        return;
    auto isBlocked_prop_id = kvstore->getpropIDBypropStr("isBlocked");
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(person_node.node_id_, isBlocked_prop_id, &value_true, txn);
}