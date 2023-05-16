MATCH (acc:Account {id: '$accountId'}), (loan: Loan {id: '$loanId'})
CREATE (acc)<-[:LoanDepositAccount {timestamp: $time, amount: $amount}]-(loan)
