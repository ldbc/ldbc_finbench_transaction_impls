#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw10(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& person_id1 = args[0];
    auto& person_id2 = args[1];
    auto& time = args[2];

    Node person_node1("Person", "id", &person_id1, kvstore, txn);
    Node person_node2("Person", "id", &person_id2, kvstore, txn);
    assert(person_node1.node_id_ != -1);
    assert(person_node2.node_id_ != -1);

    db->AddEdge(person_node1.node_id_, person_node2.node_id_, kvstore->getIDByPredicate("PERSON_GUARANTEE_PERSON"),
                {time.data_.Long}, txn);

}