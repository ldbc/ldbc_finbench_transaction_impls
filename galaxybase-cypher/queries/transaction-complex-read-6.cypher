CYPHER EXPANDCONFIG = ([transfer1, transfer2], timestamp, $truncationOrder, $truncationLimit)
MATCH (:Account {id: '$id'})<-[transfer2:AccountWithdrawAccount]-(mid:Account)
<-[transfer1:AccountTransferAccount]-(:Account)
WHERE transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime AND transfer2.amount > $threshold2 AND
transfer1.timestamp > $startTime AND transfer1.timestamp < $endTime AND transfer1.amount > $threshold1
WITH mid.id AS midId, collect(DISTINCT transfer1) AS transfer1s, collect(DISTINCT transfer2) AS transfer2s
WITH midId, apoc.coll.sum([e in transfer1s | e.amount]) AS sumEdge1Amount,
apoc.coll.sum([e in transfer2s | e.amount]) AS sumEdge2Amount, size(transfer1s) AS edgeCount
WHERE edgeCount > 3
RETURN midId, sumEdge1Amount, sumEdge2Amount
ORDER BY sumEdge2Amount DESC, toInteger(midId)