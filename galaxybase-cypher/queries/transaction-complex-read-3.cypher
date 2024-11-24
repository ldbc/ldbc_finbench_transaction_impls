OPTIONAL MATCH path = shortestPath((:Account {id: $id1 })-[transfer:AccountTransferAccount*1..16]->(:Account {id: $id2 }))
WHERE all(e IN transfer WHERE e.timestamp > $startTime AND e.timestamp < $endTime)
RETURN
CASE path IS NULL
  WHEN true THEN -1
  ELSE length(path)
END AS shortestPathLength