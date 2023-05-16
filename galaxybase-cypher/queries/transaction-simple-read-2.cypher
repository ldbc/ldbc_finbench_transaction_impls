MATCH (src:Account {id: '$id'})
OPTIONAL MATCH (src)-[edge1:AccountTransferAccount]->(dst1:Account)
WHERE $startTime < edge1.timestamp < $endTime
OPTIONAL MATCH (src)<-[edge2:AccountTransferAccount]->(dst2:Account)
WHERE $startTime < edge2.timestamp < $endTime
RETURN
    sum(edge1.amount) AS sumEdge1Amount, 
	CASE edge1 IS NULL
	  WHEN true THEN -1
	  ELSE max(edge1.amount)
	END AS maxEdge1Amount,
	count(edge1) AS numEdge1,
    sum(edge2.amount) AS sumEdge2Amount, 
	CASE edge2 IS NULL
	  WHEN true THEN -1
	  ELSE max(edge2.amount)
	END AS maxEdge2Amount,
	count(edge2) AS numEdge2
