MATCH (loan: Loan {id: $loanId}), (account:Account {id: $accountId})
CREATE (loan)-[:LoanDepositAccount {timestamp: $time, amount: $amount}]->(account)
