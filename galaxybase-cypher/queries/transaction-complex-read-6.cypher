CYPHER EXPANDCONFIG = ([transfer1, transfer2], timestamp, $truncationOrder, $truncationLimit)
MATCH (:Account {id: '$id'})<-[transfer2:AccountTransferAccount]-(mid:Account)
<-[transfer1:AccountTransferAccount]-(:Account)
WHERE transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime AND transfer2.amount > $threshold2 AND
transfer1.timestamp > $startTime AND transfer1.timestamp < $endTime AND transfer1.amount > $threshold1
WITH mid.id AS midId, sum(transfer1.amount) AS sumEdge1Amount, sum(transfer2.amount) AS sumEdge2Amount, count(transfer1) AS edgeCount
WHERE edgeCount > 3
RETURN midId, sumEdge1Amount, sumEdge2Amount
ORDER BY sumEdge2Amount DESC, midId 