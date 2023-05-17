CYPHER EXPANDCONFIG = ([edge2, edge1], timestamp, $truncationOrder, $truncationLimit)
MATCH (src:Account)-[edge1:AccountTransferAccount]->(mid:Account {id: '$id'})
OPTIONAL MATCH (mid:Account)-[edge2:AccountTransferAccount]->(dst:Account)
WHERE edge1.timestamp > $startTime AND edge1.timestamp < $endTime AND edge1.amount > $threshold
AND edge2.timestamp > $startTime AND edge2.timestamp < $endTime AND edge2.amount > $threshold
WITH COUNT(DISTINCT src) AS numSrc, COUNT(DISTINCT dst) AS numDst
RETURN numSrc, numDst, 
CASE numDst=0
  WHEN true THEN -1
  ELSE apoc.math.round(1.0 * numSrc/numDst, 3)
END AS inOutRatio