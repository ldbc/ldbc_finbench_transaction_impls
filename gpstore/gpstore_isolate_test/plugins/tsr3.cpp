#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tsr3(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double threshold = args[1].data_.Float, start_time = args[2].data_.Long,
         end_time = args[3].data_.Long;
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_IN);
  double valid_neighbor_cnt = 0.0;
  for (int i = 0; i < neighbor_len; ++i) {
    double prop[2];  // prop[0] is amount, prop[1] is createTime
    memcpy(&prop, prop_list + i * prop_len, sizeof(double) << 1);
    if (prop[1] > start_time && prop[1] < end_time && prop[0] > threshold) {
      valid_neighbor_cnt += 1;
    }
  }
  delete neighbor_list;
  delete prop_list;
  double ratio =
      neighbor_len == 0
          ? -1.0
          : std::round(valid_neighbor_cnt / neighbor_len * 1000) / 1000;
  new_result->rows_.emplace_back();
  new_result->rows_.back().values_.emplace_back(ratio);
}