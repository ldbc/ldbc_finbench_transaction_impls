MATCH (account:Account {id: $accountId}), (loan: Loan {id: $loanId})
CREATE (account)-[:AccountRepayLoan {timestamp: $time, amount: $amount}]->(loan)
