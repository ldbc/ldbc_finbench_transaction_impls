MATCH (p:Person {id: $personId})
CREATE (:Loan {id: $loanId, loanAmount: $loanAmount, balance: $balance})<-[:PersonApplyLoan {timestamp: $time}]-(p)

