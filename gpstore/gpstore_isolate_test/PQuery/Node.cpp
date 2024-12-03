
#include "Node.h"

Node::Node(TYPE_ENTITY_LITERAL_ID node_id, const KVstore *kv_store, std::shared_ptr<Transaction> txn):
    node_id_(node_id), kv_store_(kv_store), txn_(txn) {}

Node::Node(const std::string& label_string, const std::string& prop_string, const GPStore::Value *value, const KVstore *kv_store, std::shared_ptr<Transaction> txn):
    kv_store_(kv_store), txn_(txn) {
  TYPE_PROPERTY_ID prop_id = kv_store->getpropIDBypropStr(prop_string);
  TYPE_LABEL_ID label_id = kv_store->getIDByLiteral(label_string);
  TYPE_ENTITY_LITERAL_ID* node_list = nullptr;
  unsigned node_list_len = 0;
  kv_store->find_index(label_id, prop_id, value, node_list, node_list_len, txn_);
  if(node_list == nullptr) node_id_ = -1;
  else node_id_ = node_list[0];
  delete[] node_list;
}

Node::Node(TYPE_ENTITY_LITERAL_ID label_id, TYPE_PROPERTY_ID prop_id, const GPStore::Value *value, const KVstore *kv_store, std::shared_ptr<Transaction> txn):
        kv_store_(kv_store), txn_(txn) {
    TYPE_ENTITY_LITERAL_ID* node_list = nullptr;
    unsigned node_list_len = 0;
    kv_store->find_index(label_id, prop_id, value, node_list, node_list_len, txn_);
    if(node_list == nullptr) node_id_ = -1;
    else node_id_ = node_list[0];
    delete[] node_list;
}

Node::~Node() {
  for (auto x : prop_cache_) delete x.second;
}

GPStore::Value* Node::operator[](const std::string& property_string) {
  TYPE_PREDICATE_ID prop_id = kv_store_->getpropIDBypropStr(property_string);
  if (prop_cache_.find(prop_id) != prop_cache_.end()) return prop_cache_[prop_id];
  GPStore::Value* val_ptr = nullptr;
  if (!kv_store_->getnodeValueByid(node_id_, prop_id, val_ptr, txn_)) val_ptr = new GPStore::Value(GPStore::Value::Type::NO_VALUE);
  if (!val_ptr) val_ptr = new GPStore::Value(GPStore::Value::Type::NO_VALUE);
  prop_cache_[prop_id] = val_ptr;
  return val_ptr;
}

GPStore::Value* Node::operator[](TYPE_PROPERTY_ID prop_id) {
  if (prop_cache_.find(prop_id) != prop_cache_.end()) return prop_cache_[prop_id];
  GPStore::Value* val_ptr = nullptr;
  if (!kv_store_->getnodeValueByid(node_id_, prop_id, val_ptr, txn_)) val_ptr = new GPStore::Value(GPStore::Value::Type::NO_VALUE);
  if (!val_ptr) val_ptr = new GPStore::Value(GPStore::Value::Type::NO_VALUE);
  prop_cache_[prop_id] = val_ptr;
  return val_ptr;
}

void Node::GetLinkedNodes(const string& pre_str, unsigned*& nodes_list, unsigned& list_len, char edge_dir, bool no_duplicate) {
  TYPE_PREDICATE_ID pre_id = kv_store_->getIDByPredicate(pre_str);
//  TODO: to improve performance in our project, we should split these two direction cases
  if (edge_dir == Util::EDGE_OUT)
    kv_store_->getObjIDListBySubIDPreIDIV(node_id_, pre_id, nodes_list, list_len, txn_);
  else
    kv_store_->getSubIDListByObjIDPreIDIV(node_id_, pre_id, nodes_list, list_len, txn_);
}

void Node::GetLinkedNodes(TYPE_PREDICATE_ID pre_id, unsigned*& nodes_list, unsigned& list_len, char edge_dir, bool no_duplicate) {
//  TODO: to improve performance in our project, we should split these two direction cases
  if (edge_dir == Util::EDGE_OUT)
    kv_store_->getObjIDListBySubIDPreIDIV(node_id_, pre_id, nodes_list, list_len, txn_);
  else
    kv_store_->getSubIDListByObjIDPreIDIV(node_id_, pre_id, nodes_list, list_len, txn_);
}

void Node::GetLinkedNodesWithEdgeProps(const string& pre_str, unsigned*& nodes_list, long long*& prop_list,
                                       unsigned& prop_len, unsigned& list_len, char edge_dir, bool no_duplicate) {
  TYPE_PREDICATE_ID pre_id = kv_store_->getIDByPredicate(pre_str);
  GetLinkedNodesWithEdgeProps(pre_id, nodes_list, prop_list, prop_len, list_len, edge_dir, no_duplicate);
}

void Node::GetLinkedNodesWithEdgeProps(TYPE_PREDICATE_ID pre_id, unsigned*& nodes_list, long long*& prop_list,
                                       unsigned& prop_len, unsigned& list_len, char edge_dir, bool no_duplicate) {
  if (edge_dir == Util::EDGE_OUT)
    kv_store_->getobjIDxvaluesBysubIDpreID(node_id_, pre_id, nodes_list, prop_list, prop_len, list_len, no_duplicate, txn_);
  else
    kv_store_->getsubIDxvaluesByobjIDpreID(node_id_, pre_id, nodes_list, prop_list, prop_len, list_len, no_duplicate, txn_);
}

void Node::GetNeighbors(char edge_dir, const std::vector<TYPE_PREDICATE_ID> &pred_set, std::vector<GPStore::uint_32> &neighbors) {
  for (auto pre_id : pred_set) {
    unsigned* nei_list = nullptr; unsigned len = 0;
    GetLinkedNodes(pre_id, nei_list, len, edge_dir, false);
    neighbors.insert(neighbors.end(), nei_list, nei_list + len);
    delete[] nei_list;
  }
}

bool Node::operator<(const Node& rhs) const {
  return node_id_ < rhs.node_id_;
}