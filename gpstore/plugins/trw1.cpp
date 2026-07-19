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

  TYPE_ENTITY_LITERAL_ID* dst_out_neighborList = nullptr; unsigned dst_out_neighbor_len = 0;
  long long* dst_out_prop_list = nullptr; unsigned dst_out_prop_len = 0;
  dst.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", dst_out_neighborList,
                                  dst_out_prop_list, dst_out_prop_len, dst_out_neighbor_len, Util::EDGE_OUT);

  TYPE_ENTITY_LITERAL_ID* src_in_neighborList = nullptr; unsigned src_in_neighbor_len = 0;
  long long* src_in_prop_list = nullptr; unsigned src_in_prop_len = 0;
  src.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", src_in_neighborList,
                                  src_in_prop_list, src_in_prop_len, src_in_neighbor_len, Util::EDGE_IN);


  bool has_same_account = false;
  for(unsigned i = 0; i < dst_out_neighbor_len; ++i) {
    double dst_out_edge_time = GPStore::Value(dst_out_prop_list[i * dst_out_prop_len + 1]).data_.Long;
    if(dst_out_edge_time <= args[4].data_.Long || dst_out_edge_time >= args[5].data_.Long) continue;
    for(unsigned j = 0; j < src_in_neighbor_len; ++j) {
      double src_in_edge_time = GPStore::Value(src_in_prop_list[j * src_in_prop_len + 1]).data_.Long;
      if(src_in_edge_time > args[4].data_.Long && src_in_edge_time < args[5].data_.Long &&
         dst_out_neighborList[i] == src_in_neighborList[j]) {
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
              {args[3].data_.Long, args[2].data_.Long, 0}, txn);
}
