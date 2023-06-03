// TODO: implement truncation strategy
MATCH path1=shortestPath((src:Account {id: $id1})-[edge:transfer*]->(dst:Account {id: $id2}))
WHERE all(e IN edge WHERE $start_time < e.timestamp < $end_time)
RETURN length(path1) AS shortestPathLength
