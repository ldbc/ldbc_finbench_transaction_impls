#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw14(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  Node loan("Loan", "id", &args[1], kvstore, txn);
  assert(account.node_id_ != -1);
  assert(loan.node_id_ != -1);
  TYPE_PREDICATE_ID repay_pre_id = kvstore->getIDByPredicate("ACCOUNT_REPAY_LOAN");
  db->AddEdge(account.node_id_, loan.node_id_, repay_pre_id, {args[3].data_.Long, args[2].data_.Long}, txn);
}