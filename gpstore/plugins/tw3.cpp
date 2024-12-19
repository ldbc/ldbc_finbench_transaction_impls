#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& medium_id = args[0];
  auto& medium_type = args[1];
  auto& is_blocked = args[2];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Medium")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("mediumType"),
                  kvstore->getpropIDBypropStr("isBlocked"),kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("lastLoginTime"), kvstore->getpropIDBypropStr("riskLevel")

              },
              {
                  medium_id, medium_type,is_blocked,
                  {}, {}, {}
              }, node_id, txn);
}