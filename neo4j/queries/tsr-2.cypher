MATCH (src:Account {id: $id})
OPTIONAL MATCH (src)-[edge1:transfer]->(dst1:Account)
WHERE $start_time < edge1.timestamp < $end_time
OPTIONAL MATCH (src)<-[edge2:transfer]->(dst2:Account)
WHERE $start_time < edge2.timestamp < $end_time
RETURN
    sum(edge1.amount), max(edge1.amount), count(edge1),
    sum(edge2.amount), max(edge2.amount), count(edge2)
