MATCH (p1:Person {id: $pid1}), (p2:Person {id: $pid2})
WITH p1, p2
OPTIONAL MATCH (p1)-[edge1:PersonInvestCompany]->(p1Neighbor)
WHERE $startTime < edge1.timestamp < $endTime
WITH p1, p2, count(DISTINCT p1Neighbor) AS p1Neighbors
OPTIONAL MATCH (p2)-[edge2:PersonInvestCompany]->(p2Neighbor)
WHERE $startTime < edge2.timestamp < $endTime
WITH p1, p2, p1Neighbors, count(DISTINCT p2Neighbor) AS p2Neighbors
OPTIONAL MATCH (p1)-[edge3:PersonInvestCompany]->(midNeighbor:Company)<-[edge4:PersonInvestCompany]-(p2)
WHERE $startTime < edge3.timestamp < $endTime
AND $startTime < edge4.timestamp < $endTime
WITH p1Neighbors, p2Neighbors, count(DISTINCT midNeighbor) AS p12Neighbors
WITH p12Neighbors AS intersection, p1Neighbors + p2Neighbors - p12Neighbors AS union
RETURN
CASE union = 0
  WHEN true THEN 0
  ELSE round(1.0 * intersection / union * 1000) / 1000
END AS jaccardSimilarity