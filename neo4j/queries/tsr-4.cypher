MATCH (src:Account {id: $id})-[edge:transfer]->(dst:Account)
WHERE $start_time < edge.timestamp < $end_time
  AND edge.amount > $threshold
RETURN dst.id AS dstId, count(edge) AS numEdges, sum(edge.amount) AS sumAmount
