MATCH (src:Account {id: $id})<-[e1:AccountTransferAccount]-(mid:Account)-[e2:AccountTransferAccount]->(dst:Account {isBlocked: true})
WHERE src.id <> dst.id
  AND $startTime < e1.timestamp < $endTime
  AND $startTime < e2.timestamp < $endTime
RETURN DISTINCT dst.id AS dstId
ORDER BY toInteger(dstId)