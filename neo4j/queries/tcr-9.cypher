// TODO: implement truncation strategy
MATCH
  (loan:Loan)-[edge1:deposit]->(mid:Account {id: $id})-[edge2:repay]->(loan),
  (up:Account)-[edge3:transfer]->(mid)-[edge4:transfer]->(down:Account)
WHERE edge1.amount > $threshold AND $start_time < edge1.timestamp < $end_time
  AND edge2.amount > $threshold AND $start_time < edge2.timestamp < $end_time
  AND $lowerbound < edge1.amount/edge2.amount < $upperbound
  AND edge3.amount > $threshold AND $start_time < edge3.timestamp < $end_time
  AND edge4.amount > $threshold AND $start_time < edge4.timestamp < $end_time
RETURN
  round(1000 * sum(edge1.amount)/sum(edge2.amount)) / 1000 AS ratioRepay,
  round(1000 * sum(edge1.amount)/sum(edge4.amount)) / 1000 AS ratioOut,
  round(1000 * sum(edge3.amount)/sum(edge4.amount)) / 1000 AS ratioIn
