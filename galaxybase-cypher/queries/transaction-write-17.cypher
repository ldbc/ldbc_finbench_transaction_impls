MATCH (account:Account {id: '$accountId'})
OPTIONAL MATCH (account)-[:AccountRepayLoan]->(loan:Loan)-[:LoanDepositAccount]->(account)
DETACH DELETE account, loan