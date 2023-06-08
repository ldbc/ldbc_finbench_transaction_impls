OPTIONAL MATCH (dst:Account {id: $id})
WITH dst
OPTIONAL MATCH (src:Account)-[edge2:AccountTransferAccount]->(dst)
WITH dst, count(edge2) AS edge2Num
OPTIONAL MATCH (blockedSrc:Account {isBlocked: true})-[edge1:AccountTransferAccount]->(dst)
WHERE $startTime < edge1.timestamp < $endTime
  AND edge1.amount > $threshold
WITH count(edge1) AS edge1Num, edge2Num
RETURN 
CASE edge2Num=0
  WHEN true THEN -1
  ELSE round(1.0 * edge1Num/edge2Num * 1000) / 1000
END AS blockRatio
