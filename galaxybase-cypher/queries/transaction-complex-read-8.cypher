CYPHER EXPANDCONFIG = ([edge], timestamp, $truncationOrder, $truncationLimit)
MATCH (loan:Loan {id: $id})-[deposit:LoanDepositAccount]->(src:Account)
WHERE $startTime < deposit.timestamp < $endTime
WITH loan, src
MATCH p=(src)-[edge:AccountTransferAccount|AccountWithdrawAccount*1..3]->(dst:Account)
WITH loan, p, dst, [e IN relationships(p) | e.amount] AS amts
WHERE all(e IN edge WHERE $startTime < e.timestamp < $endTime)
  AND reduce(curr = head(amts), x IN tail(amts) | CASE WHEN (curr <> -1) AND (x > curr * $threshold ) THEN x ELSE -1 end) <> -1
WITH DISTINCT dst.id AS dstId, loan, collect(DISTINCT relationships(p)[-1]) AS edges, min(length(p)+1) AS minDistanceFromLoan
RETURN dstId,
round(1.0 * apoc.coll.sum([e in edges| e.amount]) / loan.loanAmount * 1000) / 1000 AS ratio,
minDistanceFromLoan
ORDER BY minDistanceFromLoan DESC, ratio DESC, toInteger(dstId)