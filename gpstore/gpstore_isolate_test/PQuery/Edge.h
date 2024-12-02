#ifndef GPSTORE_PQUERY_EDGE_H_
#define GPSTORE_PQUERY_EDGE_H_

#include "../KVstore/KVstore.h"
#include "../Util/Value.h"

class Edge {
 public:
  TYPE_EDGE_ID edge_id_;
  const KVstore* kv_store_;
  std::shared_ptr<Transaction> txn_;
  std::map<std::string, GPStore::Value*> prop_cache_;

  Edge(TYPE_EDGE_ID edge_id, const KVstore* kv_store, std::shared_ptr<Transaction> txn);
  Edge(TYPE_ENTITY_LITERAL_ID s_id, TYPE_ENTITY_LITERAL_ID o_id, TYPE_PREDICATE_ID p_id, unsigned e_index,
       const KVstore* kv_store, std::shared_ptr<Transaction> txn);
  Edge(TYPE_ENTITY_LITERAL_ID s_id, TYPE_ENTITY_LITERAL_ID o_id, const std::string& p_string, unsigned int e_index,
       const KVstore *kv_store, std::shared_ptr<Transaction> txn);
  ~Edge();
  GPStore::Value* operator[](const std::string& property_string);
  GPStore::Value* operator[](TYPE_PROPERTY_ID prop_id);
};

#endif //GPSTORE_PQUERY_EDGE_H_
