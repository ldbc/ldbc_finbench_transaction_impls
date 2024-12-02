#ifndef GPSTORE_PQUERY_NODE_H_
#define GPSTORE_PQUERY_NODE_H_

#include "../KVstore/KVstore.h"
#include "../Util/Value.h"

class Node {
 public:
  TYPE_ENTITY_LITERAL_ID node_id_;
  const KVstore* kv_store_;
  std::shared_ptr<Transaction> txn_;
  std::map<TYPE_PROPERTY_ID, GPStore::Value*> prop_cache_;

  Node() = default;
  Node(TYPE_ENTITY_LITERAL_ID node_id, const KVstore* kv_store, std::shared_ptr<Transaction> txn);
  Node(const std::string& label_string, const std::string& prop_string, const GPStore::Value* value, const KVstore* kv_store, std::shared_ptr<Transaction> txn);  // used for property 'id'
  Node(TYPE_ENTITY_LITERAL_ID label_id, TYPE_PROPERTY_ID prop_id, const GPStore::Value *value, const KVstore *kv_store, std::shared_ptr<Transaction> txn);
  Node(const KVstore *kv_store, std::shared_ptr<Transaction> txn):node_id_(INVALID_ENTITY_LITERAL_ID), kv_store_(kv_store), txn_(txn) {}
  void Goto(TYPE_ENTITY_LITERAL_ID new_node_id) {
    node_id_ = new_node_id;
    for (auto x : prop_cache_) delete x.second;
    std::map<TYPE_PROPERTY_ID, GPStore::Value*>().swap(prop_cache_);
  }
  ~Node();
  GPStore::Value* operator[](const std::string& property_string);
  GPStore::Value* operator[](TYPE_PROPERTY_ID prop_id);
  void GetLinkedNodes(const std::string&, unsigned*& nodes_list, unsigned& list_len, char edge_dir, bool no_duplicate=false);
  void GetLinkedNodes(TYPE_PREDICATE_ID pre_id, unsigned*& nodes_list, unsigned& list_len, char edge_dir, bool no_duplicate=false);
  void GetLinkedNodesWithEdgeProps(const string& pre_str, unsigned*& nodes_list, long long*& prop_list,
                                   unsigned& prop_len, unsigned& list_len, char edge_dir, bool no_duplicate=false);
  void GetLinkedNodesWithEdgeProps(TYPE_PREDICATE_ID pre_id, unsigned*& nodes_list, long long*& prop_list,
                                   unsigned& prop_len, unsigned& list_len, char edge_dir, bool no_duplicate = false);
  void GetNeighbors(char edge_dir, const std::vector<TYPE_PREDICATE_ID> &pred_set, std::vector<GPStore::uint_32> &neighbors);

  bool operator<(const Node& rhs) const;
};

#endif //GPSTORE_PQUERY_NODE_H_
