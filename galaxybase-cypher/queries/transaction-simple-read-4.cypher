MATCH (src:Account {id: '$id'})-[edge:AccountTransferAccount]->(dst:Account)
WHERE $startTime < edge.timestamp < $endTime
  AND edge.amount > $threshold
RETURN dst.id AS dstId, count(edge) AS numEdges, sum(edge.amount) AS sumAmount
ORDER BY sumAmount DESC, toInteger(dstId)
