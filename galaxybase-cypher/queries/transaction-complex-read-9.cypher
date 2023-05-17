CYPHER EXPANDCONFIG = ([edge3, edge4], timestamp, $truncationOrder, $truncationLimit)
OPTIONAL MATCH (loan:Loan)-[edge1:LoanDepositAccount]->(mid:Account {id: '$id'})-[edge2:AccountReplyLoan]->(loan),
  (up:Account)-[edge3:AccountTransferAccount]->(mid)-[edge4:AccountTransferAccount]->(down:Account)
WHERE edge1.amount > $threshold AND $startTime < edge1.timestamp < $endTime
  AND edge2.amount > $threshold AND $startTime < edge2.timestamp < $endTime
  AND edge3.amount > $threshold AND $startTime < edge3.timestamp < $endTime
  AND edge4.amount > $threshold AND $startTime < edge4.timestamp < $endTime
RETURN
CASE edge2 IS NULL
  WHEN true THEN -1
  ELSE apoc.math.round(1.0 * sum(edge1.amount)/sum(edge2.amount), 3)
END AS ratioRepay,
CASE edge4 IS NULL
  WHEN true THEN -1
  ELSE apoc.math.round(1.0 * sum(edge1.amount)/sum(edge4.amount), 3)
END AS ratioDeposit,
CASE edge4 IS NULL
  WHEN true THEN -1
  ELSE apoc.math.round(1.0 * sum(edge3.amount)/sum(edge4.amount), 3)
END AS ratioTransfer