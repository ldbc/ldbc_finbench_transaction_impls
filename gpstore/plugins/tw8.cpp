#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw8(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& person_id = args[0];
    auto& company_id = args[1];
    auto& time = args[2];
    auto& ratio = args[3];

    Node person_node("Person", "id", &person_id, kvstore, txn);
    Node company_node("Company", "id", &company_id, kvstore, txn);
    assert(person_node.node_id_ != -1);
    assert(company_node.node_id_ != -1);

    long long ratio_ll;
    memcpy(&ratio_ll, &(ratio.data_.Float), sizeof(long long));

    db->AddEdge(person_node.node_id_, company_node.node_id_, kvstore->getIDByPredicate("PERSON_INVEST_COMPANY"),
                {ratio_ll, time.data_.Long}, txn);

}