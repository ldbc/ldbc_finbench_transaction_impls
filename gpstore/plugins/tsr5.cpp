#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tsr5(const std::vector<GPStore::Value> &args,
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
  std::unordered_map<TYPE_ENTITY_LITERAL_ID, std::pair<int, double>> res_mp;
  for (int i = 0; i < neighbor_len; ++i) {
    double curAmount = LL2double(prop_list[i * prop_len]);
    long long curTime = prop_list[i * prop_len + 1];
    if (curTime > start_time && curTime < end_time && curAmount > threshold) {
      if (res_mp.count(neighbor_list[i]) == 0) {
        res_mp.emplace(neighbor_list[i], std::pair<int, double>(0, 0.0));
      }
      res_mp[neighbor_list[i]].first += 1;
      res_mp[neighbor_list[i]].second += curAmount;
    }
  }
  delete []neighbor_list;
  delete []prop_list;
  auto cmp = [](const std::tuple<long long, int, double> &a,
                const std::tuple<long long, int, double> &b) {
    if (std::get<2>(a) != std::get<2>(b)) {
      return std::get<2>(a) < std::get<2>(b);
    }
    return std::get<0>(a) > std::get<0>(b);
  };
  std::priority_queue<std::tuple<long long, int, double>,
                      std::vector<std::tuple<long long, int, double>>,
                      decltype(cmp)>
      que(cmp);
  TYPE_PREDICATE_ID local_id_prop_id = kvstore->getpropIDBypropStr("id");
  for (const auto &item : res_mp) {
    GPStore::Value *val_ptr = nullptr;
    assert(
        kvstore->getnodeValueByid(item.first, local_id_prop_id, val_ptr, txn));
    que.emplace(val_ptr->data_.Long, item.second.first,
                std::round(item.second.second * 1000) / 1000);
    delete val_ptr;
  }
  new_result->rows_.reserve(que.size());
  while (!que.empty()) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.reserve(3);
    new_result->rows_.back().values_.emplace_back(std::get<0>(que.top()));
    new_result->rows_.back().values_.emplace_back(std::get<1>(que.top()));
    new_result->rows_.back().values_.emplace_back(std::get<2>(que.top()));
    que.pop();
  }
}