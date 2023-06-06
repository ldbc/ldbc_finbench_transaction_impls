MATCH (src:Account)-[edge2:transfer]->(dst:Account {id: $id})
OPTIONAL MATCH (blockedSrc:Account {isBlocked: true})-[edge1:transfer]->(dst)
WHERE $start_time < edge1.timestamp < $end_time
  AND edge1.amount > $threshold
RETURN round(1000*count(edge1)/count(edge2)) / 1000 AS blockRatio
