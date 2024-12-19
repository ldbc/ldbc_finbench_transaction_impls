#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw15(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node loan("Loan", "id", &args[0], kvstore, txn);
  Node account("Account", "id", &args[1], kvstore, txn);
  assert(loan.node_id_ != -1);
  assert(account.node_id_ != -1);
  TYPE_PREDICATE_ID deposit_pre_id = kvstore->getIDByPredicate("LOAN_DEPOSIT_ACCOUNT");
  db->AddEdge(loan.node_id_, account.node_id_, deposit_pre_id, {args[3].data_.Long, args[2].data_.Long}, txn);
}