CYPHER EXPANDCONFIG = ([transfer], timestamp, $truncationOrder, $truncationLimit)
MATCH (person:Person {id: $id})-[own:PersonOwnAccount]->(account:Account),
p=(other:Account)-[transfer:AccountTransferAccount*1..3]->(account)
WITH p, [e IN relationships(p) | e.timestamp] AS ts, other
WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
AND all(e IN transfer WHERE e.timestamp > $startTime AND e.timestamp < $endTime)
WITH DISTINCT other
MATCH (other)<-[deposit:LoanDepositAccount]-(loan:Loan)
WHERE deposit.timestamp > $startTime AND deposit.timestamp < $endTime
RETURN other.id AS otherId, round(sum(DISTINCT loan.loanAmount) * 1000) / 1000 AS sumLoanAmount, round(sum(DISTINCT loan.balance) * 1000) / 1000 AS sumLoanBalance
ORDER BY sumLoanAmount DESC, toInteger(otherId)