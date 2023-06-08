MATCH (account:Account {id: $accountId})
WITH account
OPTIONAL MATCH (account)-[:AccountRepayLoan]->(loan1:Loan)
WITH account, loan1
OPTIONAL MATCH (loan2:Loan)-[:LoanDepositAccount]->(account)
DETACH DELETE account, loan1, loan2
