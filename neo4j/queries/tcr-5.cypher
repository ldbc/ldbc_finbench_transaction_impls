// TODO: implement truncation strategy
MATCH
  (person:Person {id: $id})-[edge1:own]->(src:Account),
  p=(src)-[edge2:transfer*1..3]->(dst:Account)
WITH p, [e IN relationships(p) | e.timestamp] AS ts
WHERE // enforce ascending order for the timestamps of transfer edges
      reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807
      // enforce that the timestamps of all edge2 edges are within the selected window
  AND all(e IN edge2 WHERE $start_time < e.timestamp < $end_time)
RETURN p AS path
ORDER BY length(p) DESC
