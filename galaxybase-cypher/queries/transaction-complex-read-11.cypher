CYPHER EXPANDCONFIG = ([guarantee], timestamp, $truncationOrder, $truncationLimit)
MATCH p=(p1:Person {id: '$id'})-[guarantee:PersonGuaranteePerson*1..5]->(pX:Person)
WHERE all(e IN relationships(p) WHERE $startTime < e.timestamp < $endTime)
UNWIND nodes(p)[1..] AS person
MATCH (person)-[:PersonApplyLoan]->(loan:Loan)
WITH DISTINCT loan
RETURN apoc.math.round(sum(loan.loanAmount), 3) AS sumLoanAmount, count(loan) AS numLoans