CYPHER EXPANDCONFIG = ([transfer], timestamp, $truncationOrder, $truncationLimit)
MATCH (person:Person {id: '$id'})-[:PersonOwnAccount]->(src:Account),
p=(srcAccount:Account)-[transfer:AccountTransferAccount*1..3]->(dst)
WITH p, [e IN relationships(p) | e.timestamp] AS ts
WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
AND src <> dst AND ALL(e IN transfer WHERE e.timestamp>$startTime AND e.timestamp<$endTime)
WITH DISTINCT nodes(p) AS pathNodes, length(p) AS pathLength
RETURN [n IN pathNodes | n.id] AS path
ORDER BY pathLength DESC