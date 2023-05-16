CYPHER EXPANDCONFIG = ([transfer2, transfer3], timestamp, $truncationOrder, $truncationLimit)
MATCH (src:Account {id: '$id1' }),(:Account{id: '$id2' })
-[transfer3:AccountTransferAccount]->(other:Account)-[transfer2:AccountTransferAccount]->(src)
WHERE transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime
AND transfer3.timestamp > $startTime AND transfer3.timestamp < $endTime
RETURN other.id AS otherId, count(transfer2) AS numEdge2, sum(transfer2.amount) AS sumEdge2Amount, max(transfer2.amount) AS maxEdge2Amount,
count(transfer3) AS numEdge3, sum(transfer3.amount) AS sumEdge3Amount, max(transfer3.amount) AS maxEdge3Amount
ORDER BY sumEdge2Amount DESC, sumEdge3Amount DESC, otherId