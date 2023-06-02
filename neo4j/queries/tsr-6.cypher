MATCH (src:Account {id: $id})<-[e1:transfer]-(mid:Account)-[e2:transfer]->(dst:Account {isBlocked: true})
WHERE src.id <> dst.id
  AND $start_time < e1.timestamp < $end_time
  AND $start_time < e2.timestamp < $end_time
RETURN collect(dst.id) AS dstId