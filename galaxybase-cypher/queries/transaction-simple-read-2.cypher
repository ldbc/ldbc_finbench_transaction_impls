MATCH (src:Account {id: '$id'})
OPTIONAL MATCH (src)-[edge1:AccountTransferAccount]->(dst1:Account)
WHERE $startTime < edge1.timestamp < $endTime
OPTIONAL MATCH (src)<-[edge2:AccountTransferAccount]->(dst2:Account)
WHERE $startTime < edge2.timestamp < $endTime
RETURN
    apoc.math.round(sum(edge1.amount), 3) AS sumEdge1Amount,
	CASE edge1 IS NULL
	  WHEN true THEN -1
	  ELSE apoc.math.round(max(edge1.amount), 3)
	END AS maxEdge1Amount,
	count(edge1) AS numEdge1,
    apoc.math.round(sum(edge2.amount), 3) AS sumEdge2Amount,
	CASE edge2 IS NULL
	  WHEN true THEN -1
	  ELSE apoc.math.round(max(edge2.amount), 3)
	END AS maxEdge2Amount,
	count(edge2) AS numEdge2
