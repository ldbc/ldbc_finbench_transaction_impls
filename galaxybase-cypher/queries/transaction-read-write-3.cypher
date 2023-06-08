BEGIN
QUERY
MATCH (src:Person {id: $srcId}), (dst:Person {id: $dstId})
RETURN CASE WHEN src.isBlocked = true OR dst.isBlocked = true THEN false ELSE true END AS isSuccess
QUERY
MATCH (src:Person {id: $srcId}), (dst:Person {id: $dstId})
CREATE (dst)<-[:PersonGuaranteePerson {timestamp: $time}]-(src)
RETURN true AS isSuccess
QUERY
CYPHER EXPANDCONFIG = ([guarantee], timestamp, $truncationOrder, $truncationLimit)
MATCH (src:Person {id: $srcId})
OPTIONAL MATCH p=(src)-[guarantee:PersonGuaranteePerson*1..5]->(pX:Person)
WHERE all(e IN relationships(p) WHERE $startTime < e.timestamp < $endTime)
UNWIND nodes(p)[1..] AS person
MATCH (person)-[:PersonApplyLoan]->(loan:Loan)
WITH DISTINCT loan
WITH sum(loan.loanAmount) AS sumLoanAmount
RETURN CASE WHEN sumLoanAmount <= $threshold THEN true ELSE false END AS isSuccess
COMMIT

BEGIN
QUERY
MATCH (src:Person {id: $srcId}), (dst:Person {id: $dstId})
SET src.isBlocked = true,  dst.isBlocked = true
RETURN true AS isSuccess
COMMIT