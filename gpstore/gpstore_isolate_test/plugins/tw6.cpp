#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw6(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& person_id = args[0];
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
  Node person_node("Person", "id", &person_id, kvstore, txn);
  assert(person_node.node_id_ != -1);
  db->AddEdge(person_node.node_id_, node_id, kvstore->getIDByPredicate("PERSON_APPLY_LOAN"), {time.data_.Long}, txn);
}