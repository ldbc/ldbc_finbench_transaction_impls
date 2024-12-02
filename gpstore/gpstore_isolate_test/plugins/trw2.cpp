#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void trw2(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node src("Account", "id", &args[0], kvstore, txn);
  Node dst("Account", "id", &args[1], kvstore, txn);
  if (src.node_id_ == -1 || dst.node_id_ == -1)
    return;
  if (src["isBlocked"]->data_.Boolean || dst["isBlocked"]->data_.Boolean)
    return;

  double start_time = args[5].data_.Long, end_time = args[6].data_.Long;
  double amountThreshold = args[4].data_.Float;
  int truncationLimit = args[8].data_.Int;
  TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[9].data_.Int);
  bool asc = true; int prop_idx = -1;
  if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
      asc = true;
      prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
      asc = false;
      prop_idx = 1;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING) {
      asc = true;
      prop_idx = 0;
    } else if (truncationOrder == TruncationOrderType::AMOUNT_DESCENDING) {
      asc = false;
      prop_idx = 0;
    }


  TYPE_ENTITY_LITERAL_ID* in_neighborList = nullptr; unsigned in_neighbor_len = 0;
  long long* in_prop_list = nullptr; unsigned in_prop_len = 0;
  dst.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", in_neighborList,
                                  in_prop_list, in_prop_len, in_neighbor_len, Util::EDGE_OUT);
  int in_numValidNeighbor = truncationReorder(in_neighborList, in_neighbor_len,
                                              in_prop_list, in_prop_len, prop_idx, truncationLimit, asc);


  TYPE_ENTITY_LITERAL_ID* out_neighborList = nullptr; unsigned out_neighbor_len = 0;
  long long* out_prop_list = nullptr; unsigned out_prop_len = 0;
  dst.GetLinkedNodesWithEdgeProps("ACCOUNT_TRANSFER_ACCOUNT", out_neighborList,
                                  out_prop_list, out_prop_len, out_neighbor_len, Util::EDGE_IN);
  int out_numValidNeighbor = truncationReorder(out_neighborList, out_neighbor_len,
                                               out_prop_list, out_prop_len, prop_idx, truncationLimit, asc);

  double in_sum = 0, out_sum = 0;

  for(unsigned i = 0; i < in_numValidNeighbor; ++i) {
    double edge_time_in = GPStore::Value(in_prop_list[i * in_prop_len + 1]).data_.Float;
    double amount_in = GPStore::Value(in_prop_list[i * in_prop_len]).data_.Float;
    if (edge_time_in > start_time && edge_time_in < end_time &&
        amount_in > amountThreshold) {
      in_sum += GPStore::Value(in_prop_list[i * in_prop_len]).data_.Float;
    }
  }
  for(unsigned j = 0; j < out_numValidNeighbor; ++j) {
    double edge_time_out = GPStore::Value(out_prop_list[j * out_prop_len + 1]).data_.Float;
    double amount_out = GPStore::Value(out_prop_list[j * out_prop_len]).data_.Float;
    if (edge_time_out > start_time && edge_time_out < end_time &&
        amount_out > amountThreshold) {
      out_sum += GPStore::Value(out_prop_list[j * out_prop_len]).data_.Float;
    }
  }

  if(in_sum / out_sum <= args[7].data_.Float) {
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