MATCH (acc:Account {id: $accountId}), (loan: Loan {id: $loanId})
CREATE (acc)<-[:deposit {timestamp: $currentTime, amount: $amt}]-(loan)
