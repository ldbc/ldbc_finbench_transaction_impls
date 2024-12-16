#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& person_id = args[0];
  auto& person_name = args[1];
  auto& is_blocked = args[2];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Person")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("personName"),
                  kvstore->getpropIDBypropStr("isBlocked"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("gender"), kvstore->getpropIDBypropStr("birthday"),
                  kvstore->getpropIDBypropStr("country"), kvstore->getpropIDBypropStr("city")
              },
              {
                  person_id, person_name, is_blocked,
                  {}, {}, {}, {}, {}
              }, node_id, txn);
}