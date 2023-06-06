// TODO: implement truncation strategy
MATCH
  (person:Person {id: $id})-[edge1:own]->(accounts:Account),
  p=(accounts)<-[edge2:transfer*1..3]-(other:Account),
  (other)<-[edge3:deposit]-(loan:Loan)
WITH p, [e IN relationships(p) | e.timestamp] AS ts, other, loan
WHERE // enforce ascending order for the timestamps of transfer edges
      reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
      // enforce that the timestamps of all edge2 and edge3 edges are within the selected window
  AND all(e IN edge2 WHERE $start_time < e.timestamp < $end_time)
  AND $start_time < edge3.timestamp < $end_time
RETURN other.id AS otherId, sum(loan.amount) AS sumLoanAmount, sum(loan.balance) AS sumLoanBalance
ORDER BY sumLoanAmount DESC
