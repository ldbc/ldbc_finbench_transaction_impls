#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw11(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& company_id1 = args[0];
    auto& company_id2 = args[1];
    auto& time = args[2];

    Node company_node1("Company", "id", &company_id1, kvstore, txn);
    Node company_node2("Company", "id", &company_id2, kvstore, txn);
    assert(company_node1.node_id_ != -1);
    assert(company_node2.node_id_ != -1);

    db->AddEdge(company_node1.node_id_, company_node2.node_id_, kvstore->getIDByPredicate("COMPANY_GUARANTEE_COMPANY"),
                {time.data_.Long}, txn);

}