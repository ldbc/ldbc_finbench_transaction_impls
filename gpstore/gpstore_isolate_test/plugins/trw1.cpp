#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void trw1(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node src("Account", "id", &args[0], kvstore, txn);
  Node dst("Account", "id", &args[1], kvstore, txn);
  if (src.node_id_ == -1 || dst.node_id_ == -1)
    return;
  if (src["isBlocked"]->data_.Boolean || dst["isBlocked"]->data_.Boolean)
    return;

  TYPE_ENTITY_LITERAL_ID* in_neighborList = nullptr; unsigned in_neighbor_len = 0;
  long long* in_prop_list = nullptr; unsigned in_prop_len = 0;
  dst.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", in_neighborList,
                                  in_prop_list, in_prop_len, in_neighbor_len, Util::EDGE_OUT);

  TYPE_ENTITY_LITERAL_ID* out_neighborList = nullptr; unsigned out_neighbor_len = 0;
  long long* out_prop_list = nullptr; unsigned out_prop_len = 0;
  src.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", out_neighborList,
                                  out_prop_list, out_prop_len, out_neighbor_len, Util::EDGE_IN);


  bool has_same_account = false;
  for(unsigned i = 0; i < in_neighbor_len; ++i) {
    double edge_time_in = GPStore::Value(in_prop_list[i * in_prop_len + 1]).data_.Float;
    if(edge_time_in <= args[4].data_.Long || edge_time_in >= args[5].data_.Long) continue;
    for(unsigned j = 0; j < out_neighbor_len; ++j) {
      double edge_time_out = GPStore::Value(out_prop_list[j * out_prop_len + 1]).data_.Float;
      if(edge_time_out > args[4].data_.Long && edge_time_out < args[5].data_.Long &&
         in_prop_list[i] == out_prop_list[j]) {
        has_same_account = true;
        break;
      }
    }

    if(has_same_account) break;
  }

  if(has_same_account) {
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(src.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    db->getKVstore()->setnodeValueByid(dst.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    return;
  }

  db->AddEdge(src.node_id_, dst.node_id_, kvstore->getIDByPredicate("ACCOUNT_TRANSFER_ACCOUNT"),
              {args[4].data_.Long, args[3].data_.Long, 0}, txn);
}