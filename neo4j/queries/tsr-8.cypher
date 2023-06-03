MATCH (c:Company {id: $id})
OPTIONAL MATCH
  (p1:Person)-[e2:invest]->(c),
  (p2:Person)-[e4:workIn]->(c),
  (com:Company)-[e3:invest]->(c),
  (c)-[e5:apply]->(loan:Loan),
  (c)-[e1:own]->(acc:Account)
RETURN
  collect(acc.id) AS accId,
  collect(p1.id) AS p1Id,
  collect(p2.id) AS p2Id,
  collect(com.id) AS comId,
  collect(loan.id) AS loanId
