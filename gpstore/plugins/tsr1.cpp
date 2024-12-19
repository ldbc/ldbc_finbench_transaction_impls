#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tsr1(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ != -1) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    new_result->rows_.back().values_.emplace_back(*account["createTime"]);
    new_result->rows_.back().values_.emplace_back(*account["isBlocked"]);
    new_result->rows_.back().values_.emplace_back(*account["accountType"]);
  }
}