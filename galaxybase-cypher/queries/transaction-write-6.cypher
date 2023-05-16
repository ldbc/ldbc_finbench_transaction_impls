MATCH (c:Company {id: '$companyId'})
CREATE (:Loan {id: '$loanId', loanAmount: $amount})<-[:CompanyApplyLoan {timestamp: $time}]-(c)
