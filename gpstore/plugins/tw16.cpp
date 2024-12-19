#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw16(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node medium("Medium", "id", &args[0], kvstore, txn);
  Node account("Account", "id", &args[1], kvstore, txn);
  assert(medium.node_id_ != -1);
  assert(account.node_id_ != -1);
  TYPE_PREDICATE_ID signIn_pre_id = kvstore->getIDByPredicate("MEDIUM_SIGN_INACCOUNT");
  db->AddEdge(medium.node_id_, account.node_id_, signIn_pre_id, {args[2].data_.Long}, txn);
}