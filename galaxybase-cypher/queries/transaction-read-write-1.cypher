BEGIN
QUERY
MATCH (src:Account {id: $srcId}), (dst:Account {id: $dstId})
RETURN CASE WHEN src.isBlocked = true OR dst.isBlocked = true THEN false ELSE true END AS isSuccess
QUERY
MATCH (src:Account {id: $srcId}), (dst:Account {id: $dstId})
CREATE (dst)<-[:AccountTransferAccount {timestamp: $time, amount: $amount}]-(src)
RETURN true AS isSuccess
QUERY
MATCH (src:Account {id: $srcId}), (dst:Account {id: $dstId}),
(dst)-[transfer3:AccountTransferAccount]->(other:Account)-[transfer2:AccountTransferAccount]->(src)
WHERE transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime
  AND transfer3.timestamp > $startTime AND transfer3.timestamp < $endTime
WITH count(other) AS vertexNum
RETURN CASE WHEN vertexNum = 0 THEN true ELSE false END AS isSuccess
COMMIT

BEGIN
QUERY
MATCH (src:Account {id: $srcId}), (dst:Account {id: $dstId})
SET src.isBlocked = true,  dst.isBlocked = true
RETURN true AS isSuccess
COMMIT