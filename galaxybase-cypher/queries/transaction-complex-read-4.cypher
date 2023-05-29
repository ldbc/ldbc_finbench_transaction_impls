CYPHER EXPANDCONFIG = ([transfer2, transfer3], timestamp, $truncationOrder, $truncationLimit)
MATCH (src:Account {id: '$id1' })-[transfer1:AccountTransferAccount]->(:Account{id: '$id2' })
-[transfer3:AccountTransferAccount]->(other:Account)-[transfer2:AccountTransferAccount]->(src)
WHERE transfer1.timestamp > $startTime AND transfer1.timestamp < $endTime
AND transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime
AND transfer3.timestamp > $startTime AND transfer3.timestamp < $endTime
WITH other.id AS otherId, collect(DISTINCT transfer2) AS transfer2s, collect(DISTINCT transfer3) AS transfer3s
RETURN otherId,
size(transfer2s) AS numEdge2, apoc.math.round(apoc.coll.sum([e in transfer2s | e.amount]), 3) AS sumEdge2Amount, apoc.math.round(apoc.coll.max([e in transfer2s | e.amount]), 3) AS maxEdge2Amount,
size(transfer3s) AS numEdge3, apoc.math.round(apoc.coll.sum([e in transfer3s | e.amount]), 3) AS sumEdge3Amount, apoc.math.round(apoc.coll.max([e in transfer3s | e.amount]), 3) AS maxEdge3Amount
ORDER BY sumEdge2Amount DESC, sumEdge3Amount DESC, toInteger(otherId)