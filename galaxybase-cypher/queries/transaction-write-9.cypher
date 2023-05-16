MATCH (acc:Account {id: '$accountId'}), (loan: Loan {id: '$loanId'})
CREATE (acc)-[:AccountReplyLoan {timestamp: $time, amount: $amount}]->(loan)
