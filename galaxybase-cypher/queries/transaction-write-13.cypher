MATCH (acc:Account {id: '$id'})
OPTIONAL MATCH (acc)-[:AccountReplyLoan]->(loan:Loan)-[:LoanDepositAccount]->(acc)
DETACH DELETE acc, loan