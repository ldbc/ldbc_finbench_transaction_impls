#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw4(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& person_id = args[0];
  auto& account_id = args[1];
  auto& time = args[2];
  auto& account_blocked = args[3];
  auto& account_type = args[4];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Account")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("isBlocked"), kvstore->getpropIDBypropStr("accountType"),
                  kvstore->getpropIDBypropStr("nickname"), kvstore->getpropIDBypropStr("phonenum"),
                  kvstore->getpropIDBypropStr("email"), kvstore->getpropIDBypropStr("freqLoginType"),
                  kvstore->getpropIDBypropStr("lastLoginTime"), kvstore->getpropIDBypropStr("accountLevel")
              },
              {
                  account_id, time, account_blocked, account_type,
                  {}, {}, {}, {}, {}, {}
              }, node_id, txn);
  Node person_node("Person", "id", &person_id, kvstore, txn);
  assert(person_node.node_id_ != -1);
  db->AddEdge(person_node.node_id_, node_id, kvstore->getIDByPredicate("PERSON_OWN_ACCOUNT"), {}, txn);
}