MATCH (src:Account {id: $id1 })-[transfer1:AccountTransferAccount]->(dst:Account{id: $id2 })
WHERE transfer1.timestamp > $startTime AND transfer1.timestamp < $endTime
WITH src, dst
MATCH (dst)-[transfer3:AccountTransferAccount]->(other:Account)-[transfer2:AccountTransferAccount]->(src)
WHERE transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime
     AND transfer3.timestamp > $startTime AND transfer3.timestamp < $endTime
WITH DISTINCT src, other, dst
MATCH (src)<-[transfer2:AccountTransferAccount]-(other)
WHERE transfer2.timestamp > $startTime AND transfer2.timestamp < $endTime
WITH src, other, dst, count(transfer2) AS numEdge2, round(sum(transfer2.amount) * 1000) / 1000 AS sumEdge2Amount, round(max(transfer2.amount) * 1000) / 1000 AS maxEdge2Amount
MATCH (other)<-[transfer3:AccountTransferAccount]-(dst)
WHERE transfer3.timestamp > $startTime AND transfer3.timestamp < $endTime
WITH other.id AS otherId, numEdge2, sumEdge2Amount, maxEdge2Amount,
     round(count(transfer3) * 1000) / 1000 AS numEdge3, round(sum(transfer3.amount) * 1000) / 1000 AS sumEdge3Amount, round(max(transfer3.amount) * 1000) / 1000 AS maxEdge3Amount
RETURN otherId, numEdge2, sumEdge2Amount, maxEdge2Amount,
       numEdge3, sumEdge3Amount, maxEdge3Amount
ORDER BY sumEdge2Amount DESC, sumEdge3Amount DESC, toInteger(otherId)
