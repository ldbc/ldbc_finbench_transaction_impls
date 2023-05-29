CYPHER EXPANDCONFIG = ([transfer], timestamp, $truncationOrder, $truncationLimit)
MATCH (person:Person {id: '$id'})-[own:PersonOwnAccount]->(account:Account),
p=(other:Account)-[transfer:AccountTransferAccount*1..3]->(account)
WITH p, [e IN relationships(p) | e.timestamp] AS ts, other
WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
AND all(e IN transfer WHERE e.timestamp > $startTime AND e.timestamp < $endTime)
WITH DISTINCT other
MATCH (other)<-[deposit:LoanDepositAccount]-(loan:Loan)
WHERE deposit.timestamp > $startTime AND deposit.timestamp < $endTime
RETURN other.id AS otherId, apoc.math.round(sum(DISTINCT loan.loanAmount), 3) AS sumLoanAmount, apoc.math.round(sum(DISTINCT loan.balance), 3) AS sumLoanBalance
ORDER BY sumLoanAmount DESC, toInteger(otherId)