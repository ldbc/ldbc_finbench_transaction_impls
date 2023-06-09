MATCH (c:Company {id: $companyId})
CREATE (:Loan {id: $loanId, loanAmount: $loanAmount, balance: $balance})<-[:CompanyApplyLoan {timestamp: $time}]-(c)