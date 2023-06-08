MATCH (dst:Account {id: $id})<-[edge:AccountTransferAccount]-(src:Account)
WHERE $startTime < edge.timestamp < $endTime
  AND edge.amount > $threshold
RETURN src.id AS srcId, count(edge) AS numEdges, round(sum(edge.amount) * 1000) / 1000 AS sumAmount
ORDER BY sumAmount DESC, toInteger(srcId)
