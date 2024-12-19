#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void tsr6(const std::vector<GPStore::Value> &args,
                      std::unique_ptr<PTempResult> &new_result,
                      const KVstore *kvstore, Database *db,
                      std::shared_ptr<Transaction> txn) {
  Node account("Account", "id", &args[0], kvstore, txn);
  if (account.node_id_ == -1)
    return;
  double start_time = args[1].data_.Long, end_time = args[2].data_.Long;
  TYPE_PREDICATE_ID local_transfer_pre_id =
      kvstore->getIDByPredicate("ACCOUNT_TRANSFER_ACCOUNT");
  TYPE_ENTITY_LITERAL_ID *neighbor_list = nullptr;
  unsigned neighbor_len = 0;
  long long *prop_list = nullptr;
  unsigned prop_len = 0;
  // one-hop
  std::unordered_set<TYPE_ENTITY_LITERAL_ID> valid_neigh_st;
  account.GetLinkedNodesWithEdgeProps(local_transfer_pre_id, neighbor_list,
                                      prop_list, prop_len, neighbor_len,
                                      Util::EDGE_IN);
  for (int i = 0; i < neighbor_len; ++i) {
    long long create_time = prop_list[i * prop_len + 1];
    if (create_time > start_time && create_time < end_time) {
      valid_neigh_st.insert(neighbor_list[i]);
    }
  }
  delete []neighbor_list;
  delete []prop_list;
  // two-hop
  std::set<long long> res_id_st;
  std::unordered_set<TYPE_ENTITY_LITERAL_ID> candidate_st{account.node_id_};
  for (auto valid_neigh : valid_neigh_st) {
    neighbor_len = prop_len = 0;
    kvstore->getobjIDxvaluesBysubIDpreID(valid_neigh, local_transfer_pre_id,
                                         neighbor_list, prop_list, prop_len,
                                         neighbor_len, false, txn);
    for (int i = 0; i < neighbor_len; ++i) {
      if (candidate_st.count(neighbor_list[i])) {
        continue;
      }
      long long create_time = prop_list[i * prop_len + 1];
      if (create_time > start_time && create_time < end_time) {
        candidate_st.insert(neighbor_list[i]);
        Node candidate_node(neighbor_list[i], kvstore, txn);
        if (candidate_node["isBlocked"]->data_.Boolean) {
          res_id_st.insert(candidate_node["id"]->data_.Long);
        }
      }
    }
    delete []neighbor_list;
    delete []prop_list;
  }
  new_result->rows_.reserve(res_id_st.size());
  for (auto res_id : res_id_st) {
    new_result->rows_.emplace_back();
    new_result->rows_.back().values_.emplace_back(res_id);
  }
}