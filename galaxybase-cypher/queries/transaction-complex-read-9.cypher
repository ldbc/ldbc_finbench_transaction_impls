CYPHER EXPANDCONFIG = ([edge3, edge4], timestamp, $truncationOrder, $truncationLimit)
OPTIONAL MATCH (loan1:Loan)-[edge1:LoanDepositAccount]->(mid:Account {id: $id})
WHERE edge1.amount > $threshold AND $startTime < edge1.timestamp < $endTime
WITH sum(edge1.amount) AS edge1Amount
OPTIONAL MATCH (mid:Account {id: $id})-[edge2:AccountRepayLoan]->(loan2:Loan)
WHERE edge2.amount > $threshold AND $startTime < edge2.timestamp < $endTime
WITH edge1Amount, sum(edge2.amount) AS edge2Amount
OPTIONAL MATCH (up:Account)-[edge3:AccountTransferAccount]->(mid:Account {id: $id})
WHERE edge3.amount > $threshold AND $startTime < edge3.timestamp < $endTime
WITH edge1Amount, edge2Amount, sum(edge3.amount) AS edge3Amount
OPTIONAL MATCH (mid:Account {id: $id})-[edge4:AccountTransferAccount]->(down:Account)
WHERE edge4.amount > $threshold AND $startTime < edge4.timestamp < $endTime
WITH edge1Amount, edge2Amount, edge3Amount, sum(edge4.amount) AS edge4Amount
RETURN
CASE edge2Amount = 0
  WHEN true THEN -1
  ELSE round(1.0 * edge1Amount/edge2Amount * 1000) / 1000
END AS ratioRepay,
CASE edge4Amount = 0
  WHEN true THEN -1
  ELSE round(1.0 * edge1Amount/edge4Amount * 1000) / 1000
END AS ratioDeposit,
CASE edge4Amount = 0
  WHEN true THEN -1
  ELSE round(1.0 * edge3Amount/edge4Amount * 1000) / 1000
END AS ratioTransfer