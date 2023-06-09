BEGIN
QUERY
MATCH (src:Account {id: $srcId}), (dst:Account {id: $dstId})
RETURN CASE WHEN src.isBlocked = true OR dst.isBlocked = true THEN false ELSE true END AS isSuccess
QUERY
MATCH (src:Account {id: $srcId}), (dst:Account {id: $dstId})
CREATE (dst)<-[:AccountTransferAccount {timestamp: $time, amount: $amount}]-(src)
RETURN true AS isSuccess
QUERY
CYPHER EXPANDCONFIG = ([edge2, edge1], timestamp, $truncationOrder, $truncationLimit)
MATCH (src:Account)-[edge1:AccountTransferAccount]->(mid:Account {id: $srcId})
OPTIONAL MATCH (mid)-[edge2:AccountTransferAccount]->(dst:Account)
WHERE edge1.timestamp > $startTime AND edge1.timestamp < $endTime AND edge1.amount > $amountThreshold
AND edge2.timestamp > $startTime AND edge2.timestamp < $endTime AND edge2.amount > $amountThreshold
WITH COUNT(src) AS numSrc, COUNT(dst) AS numDst
WITH CASE numDst=0 WHEN true THEN -1 ELSE round(1.0 * numSrc/numDst * 1000) / 1000 END AS ratio
WITH CASE ratio <= $ratioThreshold WHEN true THEN true ELSE false END AS srcIsSuccess
MATCH (src:Account)-[edge1:AccountTransferAccount]->(mid:Account {id: $dstId})
OPTIONAL MATCH (mid)-[edge2:AccountTransferAccount]->(dst:Account)
WHERE edge1.timestamp > $startTime AND edge1.timestamp < $endTime AND edge1.amount > $amountThreshold
AND edge2.timestamp > $startTime AND edge2.timestamp < $endTime AND edge2.amount > $amountThreshold
WITH srcIsSuccess, COUNT(src) AS numSrc, COUNT(dst) AS numDst
WITH srcIsSuccess, CASE numDst=0 WHEN true THEN -1 ELSE round(1.0 * numSrc/numDst * 1000) / 1000 END AS ratio
WITH srcIsSuccess, CASE ratio <= $ratioThreshold WHEN true THEN true ELSE false END AS dstIsSuccess
RETURN CASE WHEN srcIsSuccess AND dstIsSuccess THEN true ELSE false END AS isSuccess
COMMIT

BEGIN
QUERY
MATCH (src:Account {id: $srcId}), (dst:Account {id: $dstId})
SET src.isBlocked = true,  dst.isBlocked = true
RETURN true AS isSuccess
COMMIT