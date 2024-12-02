#ifndef PQUERY_PSHORTESTPATH_H
#define PQUERY_PSHORTESTPATH_H

#include "../KVstore/KVstore.h"
#include "../Util/Value.h"
#include "../Value/Expression.h"

class PShortestPath
{
public:
    static GPStore::Value shortestPath(TYPE_ENTITY_LITERAL_ID src_node_id, TYPE_ENTITY_LITERAL_ID tgt_node_id, const GPStore::EdgePattern& EdgeInfo, const KVstore*kvstore, shared_ptr<Transaction> txn = nullptr);

};

#endif