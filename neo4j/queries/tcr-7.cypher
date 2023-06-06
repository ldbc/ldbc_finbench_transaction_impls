// TODO: implement truncation strategy
MATCH (src:Account)-[edge1:transfer|withdraw]->(mid:Account {id: $id})-[edge2:transfer|withdraw]->(dst:Account)
WHERE $start_time < edge1.timestamp < $end_time AND edge1.amount > $threshold
  AND $start_time < edge2.timestamp < $end_time AND edge2.amount > $threshold
RETURN count(src) AS numSrc, count(dst) AS numDst, round(1000*sum(edge1.amount)/sum(edge2.amount)) / 1000 AS inOutRatio
