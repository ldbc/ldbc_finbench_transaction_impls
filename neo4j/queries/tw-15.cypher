MATCH (loan:Loan {id: $id})
DETACH DELETE loan
