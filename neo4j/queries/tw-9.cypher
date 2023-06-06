MATCH (acc:Account {id: $accountId}), (loan: Loan {id: $loanId})
CREATE (acc)-[:repay {timestamp: $currentTime, amount: $amt}]->(loan)
