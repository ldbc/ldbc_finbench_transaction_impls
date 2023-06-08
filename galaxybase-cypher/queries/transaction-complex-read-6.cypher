CYPHER EXPANDCONFIG = ([transfer1, transfer2], timestamp, $truncationOrder, $truncationLimit)
MATCH (:Account {id: $id})<-[transfer2:AccountWithdrawAccount]-(mid:Account)
WHERE transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime AND transfer2.amount > $threshold2
WITH mid, sum(transfer2.amount) as sumEdge2Amount
MATCH (mid)<-[transfer1:AccountTransferAccount]-(:Account)
WHERE transfer1.timestamp > $startTime AND transfer1.timestamp < $endTime AND transfer1.amount > $threshold1
WITH mid.id AS midId, sumEdge2Amount, sum(transfer1.amount) as sumEdge1Amount, count(transfer1.amount) as numEdge1
WHERE numEdge1 > 3
RETURN midId, round(sumEdge1Amount * 1000) / 1000, round(sumEdge2Amount * 1000) / 1000
ORDER BY sumEdge2Amount DESC, toInteger(midId)