#include "../PQuery/PProcedure.h"
using namespace std;

extern "C" void trw3(const std::vector<GPStore::Value> &args, std::unique_ptr<PTempResult> &new_result, \
const KVstore *kvstore, Database* db, std::shared_ptr<Transaction> txn) {
  Node src("Person", "id", &args[0], kvstore, txn);
  Node dst("Person", "id", &args[1], kvstore, txn);
  if (src.node_id_ == -1 || dst.node_id_ == -1)
    return;
  if (src["isBlocked"]->data_.Boolean || dst["isBlocked"]->data_.Boolean)
    return;

  double start_time = args[4].data_.Long, end_time = args[5].data_.Long;
  int truncationLimit = args[6].data_.Int;
  TruncationOrderType truncationOrder = static_cast<TruncationOrderType>(args[7].data_.Int);

  bool asc = true; int prop_idx = -1;
  if (truncationOrder == TruncationOrderType::TIMESTAMP_ASCENDING) {
    asc = true;
  } else if (truncationOrder == TruncationOrderType::TIMESTAMP_DESCENDING) {
    asc = false;
  } else if (truncationOrder == TruncationOrderType::AMOUNT_ASCENDING)
    assert(false);


  auto sumLoanAmount = 0;
  std::unordered_set<TYPE_ENTITY_LITERAL_ID> dst_set, src_set{src.node_id_}, visited;
  while(!src_set.empty()) {
    for(auto vid : src_set) {
      auto v = GPStore::Value((int)vid);
      Node cur("Person", "id", &v, kvstore, txn);
      TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
      long long *prop_list = nullptr;  unsigned prop_len = 0;
      cur.GetLinkedNodesWithEdgeProps("PERSON_GUARANTEE_PERSON", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
      int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 0, truncationLimit, asc);

      for(int i = 0; i < numValidNeighbor; ++i) {
        double edge_time = GPStore::Value(prop_list[i * prop_len + 1]).data_.Float;
        if(edge_time > start_time && edge_time < end_time &&
           visited.find(neighborList[i]) == visited.end()) {
          visited.insert(neighborList[i]);
          dst_set.insert(neighborList[i]);
        }
      }
    }

    swap(src_set, dst_set);
    dst_set.clear();
  }

  for(auto vid : visited) {
    auto v = GPStore::Value((int)vid);
    Node cur("Person", "id", &v, kvstore, txn);
    TYPE_ENTITY_LITERAL_ID *neighborList = nullptr;  unsigned neighbor_len = 0;
    long long *prop_list = nullptr;  unsigned prop_len = 0;
    cur.GetLinkedNodesWithEdgeProps("PERSON_APPLY_LOAN", neighborList, prop_list, prop_len, neighbor_len, Util::EDGE_OUT);
    int numValidNeighbor = truncationReorder(neighborList, neighbor_len, prop_list, prop_len, 1, truncationLimit, asc);

    for(int i = 0; i < numValidNeighbor; ++i) {
      sumLoanAmount += GPStore::Value(prop_list[i * prop_len]).data_.Float;
      if(sumLoanAmount > args[3].data_.Float) {
        break;
      }
    }
  }


  if(sumLoanAmount > args[3].data_.Float) {
    auto value_true = GPStore::Value(true);
    db->getKVstore()->setnodeValueByid(src.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    db->getKVstore()->setnodeValueByid(dst.node_id_, kvstore->getpropIDBypropStr("isBlocked"),
                                       &value_true, txn);
    return;
  }

  db->AddEdge(src.node_id_, dst.node_id_, kvstore->getIDByPredicate("PERSON_GUARANTEE_PERSON"),
              {args[2].data_.Long}, txn);
}