// TODO: implement truncation strategy
MATCH (person:Person {id: $id})
   -[edge1:own]->(pAcc:Account)
   -[edge2:transfer]->(compAcc:Account)
  <-[edge3:own]-(company:Company)
WHERE $start_time < edge2.timestamp < $end_time
RETURN compAcc.id AS compAccountId, sum(edge2.amount) AS sumEdge2Amount
ORDER BY sumEdge2Amount DESC
