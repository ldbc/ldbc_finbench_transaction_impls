// TODO: implement truncation strategy
MATCH
  (src:Account {id: $id1})-[edge1:transfer]->(dst:Account {id: $id2}),
  (src)<-[edge2:transfer]-(other:Account)-[edge3:transfer]->(dst)
WHERE $start_time < edge1.timestamp < $end_time
  AND $start_time < edge2.timestamp < $end_time
  AND $start_time < edge3.timestamp < $end_time
WITH
  other.id AS otherId,
  count(edge2) AS numEdge2, sum(edge2.amount) AS sumEdge2Amount, max(edge2.amount) AS maxEdge2Amount,
  count(edge3) AS numEdge3, sum(edge3.amount) AS sumEdge3Amount, max(edge3.amount) AS maxEdge3Amount
// preorder results descendingly and collect them into a list so the first item in the list of the top one
ORDER BY sumEdge2Amount+sumEdge3Amount DESC
WITH collect({otherId: otherId, numEdge2: numEdge2, sumEdge2Amount: sumEdge2Amount, maxEdge2Amount: maxEdge2Amount, numEdge3: numEdge3, sumEdge3Amount: sumEdge3Amount, maxEdge3Amount: maxEdge3Amount}) AS results
// return -1
WITH coalesce(head(results), {otherId: -1, numEdge2: 0, sumEdge2Amount: 0, maxEdge2Amount: 0, numEdge3: 0, sumEdge3Amount: 0, maxEdge3Amount: 0}) AS top
RETURN top.otherId, top.numEdge2, top.sumEdge2Amount, top.maxEdge2Amount, top.numEdge3, top.sumEdge3Amount, top.maxEdge3Amount
