// TODO: implement truncation strategy
MATCH
  p=(account:Account {id: $id})-[edge1:transfer*1..3]->(other:Account),
  (other)<-[edge2:signIn]-(medium:Medium {isBlocked: true})
WITH p, [e IN relationships(p) | e.timestamp] AS ts, other, medium
WHERE // enforce ascending order for the timestamps of transfer edges
      reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
      // enforce that the timestamps of all edge1 and edge2 edges are within the selected window
  AND all(e IN edge1 WHERE $start_time < e.timestamp < $end_time)
  AND $start_time < edge2.timestamp < $end_time
RETURN other.id AS otherId, length(p) AS accountDistance, medium.id AS mediumId, medium.type AS mediumType
ORDER BY accountDistance ASC
