// TODO: implement truncation strategy
MATCH
  (loan:Loan {id: $id})-[edge1:deposit]->(src:Account),
  p=(src)-[edge234:transfer|withdraw*1..3]->(dst:Account)
WITH loan, p, dst, [e IN relationships(p) | e.amount] AS amts
WHERE // enforce that the timestamps of edge1 and all edge234 edges are within the selected window
      $start_time < edge1.timestamp < $end_time
  AND all(e IN edge234 WHERE $start_time < e.timestamp < $end_time)
  AND reduce(curr = head(amts), x IN tail(amts) | CASE WHEN (curr <> -1) AND (x > curr*$threshold) THEN x ELSE -1 end) <> -1
// extract the last edges of each path
WITH loan, length(p)+1 AS distanceFromLoan, dst, sum(relationships(p)[-1].amount) AS inflow
RETURN dst.id AS dstId, round(1000 * inflow/loan.loanAmount) / 1000 AS ratio, distanceFromLoan
ORDER BY distanceFromLoan DESC, ratio DESC
