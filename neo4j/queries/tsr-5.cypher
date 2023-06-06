MATCH (dst:Account {id: $id})<-[edge:transfer]-(src:Account)
WHERE $start_time < edge.timestamp < $end_time
  AND edge.amount > $threshold
RETURN src.id AS srcId, count(edge) AS numEdges, sum(edge.amount) AS sumAmount
