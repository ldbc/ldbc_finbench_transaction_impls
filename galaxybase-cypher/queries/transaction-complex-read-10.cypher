MATCH (p1:Person {id: '$pid1'}), (p2:Person {id: '$pid2'})
WITH 
    size((p1)-[:PersonInvestCompany]->()) AS p1Neighbors, 
    size((p2)-[:PersonInvestCompany]->()) AS p2Neighbors,
    size((p1)-[:PersonInvestCompany]->(:Company)<-[:PersonInvestCompany]-(p2)) AS p12Neighbors
WITH p12Neighbors AS intersection, p1Neighbors + p2Neighbors - p12Neighbors AS union
RETURN
CASE union = 0
  WHEN true THEN 0
  ELSE round(1000 * intersection / union) / 1000
END AS jaccardSimilarity