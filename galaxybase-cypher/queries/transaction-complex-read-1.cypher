CYPHER EXPANDCONFIG = ([transfer], timestamp, $truncationOrder, $truncationLimit)
MATCH p=(account:Account{id: $id })-[transfer:AccountTransferAccount*1..3]->(other:Account),
(other)<-[signIn:MediumSignInAccount]-(medium:Medium {isBlocked: true})
WITH p, [e IN relationships(p) | e.timestamp] AS ts, other, medium, transfer
WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
AND all(e IN transfer WHERE e.timestamp > $startTime AND e.timestamp < $endTime)
AND signIn.timestamp > $startTime AND signIn.timestamp < $endTime
RETURN DISTINCT other.id AS otherId, length(p) AS accountDistance, medium.id AS mediumId, medium.type AS mediumType
ORDER BY accountDistance, toInteger(otherId), toInteger(mediumId)