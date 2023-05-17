CYPHER EXPANDCONFIG = ([guarantee], timestamp, $truncationOrder, $truncationLimit)
MATCH p=(p1:Person {id: '12'})-[guarantee:PersonGuaranteePerson*1..5]->(pX:Person)
WHERE all(e IN relationships(p) WHERE $startTime < e.timestamp < $endTime)
UNWIND nodes(p)[1..] AS person
MATCH (person)-[:PersonApplyLoan]->(loan:Loan)
WITH DISTINCT loan
RETURN sum(loan.loanAmount) AS sumLoanAmount, count(loan) AS numLoans