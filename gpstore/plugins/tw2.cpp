#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tw2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  auto& company_id = args[0];
  auto& company_name = args[1];
  auto& is_blocked = args[2];

  unsigned node_id;
  db->AddNode({kvstore->getIDByLiteral("Company")},
              {
                  kvstore->getpropIDBypropStr("id"), kvstore->getpropIDBypropStr("CompanyName"),
                  kvstore->getpropIDBypropStr("isBlocked"), kvstore->getpropIDBypropStr("createTime"),
                  kvstore->getpropIDBypropStr("country"), kvstore->getpropIDBypropStr("city"), kvstore->getpropIDBypropStr("business"),
                  kvstore->getpropIDBypropStr("description"), kvstore->getpropIDBypropStr("url")
              },
              {
                  company_id, company_name, is_blocked,
                  {}, {}, {}, {}, {}, {}
              }, node_id, txn);
}