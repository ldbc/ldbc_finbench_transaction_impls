MATCH (acc:Account {id: $id})
OPTIONAL MATCH (acc)-[:repay]->(loan:Loan)-[:deposit]->(acc)
DETACH DELETE acc, loan
