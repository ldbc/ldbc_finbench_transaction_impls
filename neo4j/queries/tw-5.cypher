MATCH (p:Person {id: $personId})
CREATE (:Loan {id: $loanId, loanAmount: $amount})<-[:apply {timestamp: $currentTime}]-(p)
