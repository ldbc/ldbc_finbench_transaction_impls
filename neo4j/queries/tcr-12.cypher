// TODO: implement truncation strategy
MATCH path=(p1:Person {id: $id})-[:guarantee*]->(pX:Person)
WHERE all(e IN relationships(p) WHERE $start_time < e.timestamp < $end_time)
// unwind the person nodes in the path except for the starting node
UNWIND nodes(path)[1..] AS person
MATCH (person)-[:apply]->(loan:Loan)
RETURN sum(loan.loanAmount) AS sumLoanAmount, count(loan) AS numLoans
