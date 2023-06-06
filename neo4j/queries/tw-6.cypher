MATCH (c:Company {id: $companyId})
CREATE (:Loan {id: $loanId, loanAmount: $amount})<-[:apply {timestamp: $currentTime}]-(p)
