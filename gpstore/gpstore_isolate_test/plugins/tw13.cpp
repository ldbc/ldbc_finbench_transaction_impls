#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw13(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node account_1("Account", "id", &args[0], kvstore, txn);
  Node account_2("Account", "id", &args[1], kvstore, txn);
  assert(account_1.node_id_ != -1);
  assert(account_2.node_id_ != -1);
  TYPE_PREDICATE_ID withdraw_pre_id = kvstore->getIDByPredicate("ACCOUNT_WITHDRAW_ACCOUNT");
  db->AddEdge(account_1.node_id_, account_2.node_id_, withdraw_pre_id, {args[3].data_.Long, args[2].data_.Long}, txn);
}