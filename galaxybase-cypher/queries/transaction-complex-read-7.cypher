CYPHER EXPANDCONFIG = ([edge2, edge1], timestamp, $truncationOrder, $truncationLimit)
MATCH (src:Account)-[edge1:AccountTransferAccount]->(mid:Account {id: '$id'})
WHERE edge1.timestamp > $startTime AND edge1.timestamp < $endTime AND edge1.amount > $threshold
OPTIONAL MATCH (mid:Account {id: '$id'})-[edge2:AccountTransferAccount]->(dst:Account)
WHERE edge2.timestamp > $startTime AND edge2.timestamp < $endTime AND edge2.amount > $threshold
WITH collect(DISTINCT src) AS srcs, collect(DISTINCT dst) AS dsts, collect(DISTINCT edge1) AS edge1s, collect(DISTINCT edge2) AS edge2s
RETURN size(srcs) AS numSrc, size(dsts) AS numDst,
CASE size(edge2s)=0
  WHEN true THEN -1
  ELSE apoc.math.round(1.0 * apoc.coll.sum([e in edge1s | e.amount])/apoc.coll.sum([e in edge2s | e.amount]), 3)
END AS inOutRatio