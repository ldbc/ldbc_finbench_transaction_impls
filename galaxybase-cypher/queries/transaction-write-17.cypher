MATCH (account:Account {id: $accountId})
WITH account
OPTIONAL MATCH (account)-[:AccountRepayLoan|LoanDepositAccount]-(loan:Loan)
DETACH DELETE account, loan
