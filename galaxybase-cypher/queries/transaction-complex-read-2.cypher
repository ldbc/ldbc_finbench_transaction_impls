CYPHER EXPANDCONFIG = ([transfer], timestamp, $truncationOrder, $truncationLimit)
MATCH (person:Person {id: '$id'})-[owm:PersonOwnAccount]->(account:Account),
p=(account)<-[transfer:AccountTransferAccount*1..3]-(other:Account)
WITH p, [e IN relationships(p) | e.timestamp] AS ts, other
WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
AND all(e IN transfer WHERE e.timestamp > $startTime AND e.timestamp < $endTime)
WITH DISTINCT other
MATCH (other)<-[deposit:LoanDepositAccount]-(loan:Loan)
WHERE deposit.timestamp > $startTime AND deposit.timestamp < $endTime
RETURN other.id AS otherId, sum(DISTINCT loan.loanAmount) AS sumLoanAmount, count(DISTINCT loan.balance) AS sumLoanBalance
ORDER BY sumLoanAmount DESC, otherId