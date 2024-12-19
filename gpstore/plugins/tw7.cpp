#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw7(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
    auto& company_id = args[0];
    auto& loan_id = args[1];
    auto& loan_amount = args[2];
    auto& balance = args[3];
    auto& time = args[4];

    unsigned node_id;
    db->AddNode({kvstore->getIDByLiteral("Loan")},
                {
                    kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("loanAmount"),
                    kvstore->getpropIDBypropStr("balance"), kvstore->getpropIDBypropStr("createTime"),
                    kvstore->getpropIDBypropStr("loanUsage"), kvstore->getpropIDBypropStr("interestRate")
                },
                {
                    loan_id, loan_amount, balance, {}, {}, {}
                }, node_id, txn);

    Node company_node("Company", "id", &company_id, kvstore, txn);
    assert(company_node.node_id_ != -1);
    db->AddEdge(company_node.node_id_, node_id, kvstore->getIDByPredicate("COMPANY_APPLY_LOAN"), {loan_amount.data_.Long, time.data_.Long}, txn);
}