OPTIONAL MATCH (src:Account {id: $id})
WITH src
OPTIONAL MATCH (src)-[edge1:AccountTransferAccount]->(dst1:Account)
WHERE $startTime < edge1.timestamp < $endTime
WITH src,
    round(sum(edge1.amount) * 1000) / 1000 AS sumEdge1Amount,
	CASE edge1 IS NULL
	  WHEN true THEN -1
	  ELSE round(max(edge1.amount) * 1000) / 1000
	END AS maxEdge1Amount,
	count(edge1) AS numEdge1
OPTIONAL MATCH (src)<-[edge2:AccountTransferAccount]-(dst2:Account)
WHERE $startTime < edge2.timestamp < $endTime
WITH sumEdge1Amount, maxEdge1Amount, numEdge1,
    round(sum(edge2.amount) * 1000) / 1000 AS sumEdge2Amount,
	CASE edge2 IS NULL
	  WHEN true THEN -1
	  ELSE round(max(edge2.amount) * 1000) / 1000
	END AS maxEdge2Amount,
	count(edge2) AS numEdge2
RETURN
    sumEdge1Amount,
	maxEdge1Amount,
	numEdge1,
    sumEdge2Amount,
	maxEdge2Amount,
	numEdge2
