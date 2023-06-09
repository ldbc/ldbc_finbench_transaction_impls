CYPHER EXPANDCONFIG = ([edge2, edge1], timestamp, $truncationOrder, $truncationLimit)
MATCH (mid:Account {id: $id})
WITH mid
OPTIONAL MATCH (src:Account)-[edge1:AccountTransferAccount]->(mid)
WHERE edge1.timestamp > $startTime AND edge1.timestamp < $endTime AND edge1.amount > $threshold
WITH mid, count(DISTINCT src) as numSrc, sum(edge1.amount) as amountSrc
OPTIONAL MATCH (mid)-[edge2:AccountTransferAccount]->(dst:Account)
WHERE edge2.timestamp > $startTime AND edge2.timestamp < $endTime AND edge2.amount > $threshold
WITH numSrc, amountSrc, count(DISTINCT dst) as numDst, sum(edge2.amount) as amountDst
RETURN numSrc, numDst,
CASE numDst=0
  WHEN true THEN -1
  ELSE round(1.0 * amountSrc / amountDst * 1000) / 1000
END AS inOutRatio