#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tsr2(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double start_time = args[1].data_.Long, end_time = args[2].data_.Long;
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  auto calculate_stats = [&]() -> tuple<double, double, long long> {
    double sum_amount = 0.0, max_amount = -1;
    long long transfer_count = 0;
    for (int i = 0; i < neighbor_len; ++i) {
      double curAmount = LL2double(prop_list[i * prop_len]);
      long long curTime = prop_list[i * prop_len + 1];
      if (curTime > start_time && curTime < end_time) {
        sum_amount += curAmount;
        max_amount = max(max_amount, curAmount);
        transfer_count += 1;
      }
    }
    sum_amount = std::round(sum_amount * 1000) / 1000;
    max_amount = std::round(max_amount * 1000) / 1000;
    return {sum_amount, max_amount, transfer_count};
  };
  // transfer-outs
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_OUT);
  auto res1 = calculate_stats();
  new_result->rows_.emplace_back();
  new_result->rows_.back().values_.reserve(6);
  new_result->rows_.back().values_.emplace_back(std::get<0>(res1));
  new_result->rows_.back().values_.emplace_back(std::get<1>(res1));
  new_result->rows_.back().values_.emplace_back(std::get<2>(res1));
  delete []neighbor_list;
  delete []prop_list;
  neighbor_len = prop_len = 0;
  // transfer-ins
  account.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_IN);
  auto res2 = calculate_stats();
  delete []neighbor_list;
  delete []prop_list;
  new_result->rows_.back().values_.emplace_back(std::get<0>(res2));
  new_result->rows_.back().values_.emplace_back(std::get<1>(res2));
  new_result->rows_.back().values_.emplace_back(std::get<2>(res2));
}