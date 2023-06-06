MATCH
  (p1:Person {id: $id1})-[edge1:invest]->(m1:Company),
  (p2:Person {id: $id2})-[edge2:invest]->(m2:Company)
WHERE $start_time < edge1.timestamp < $end_time
  AND $start_time < edge2.timestamp < $end_time
WITH gds.similarity.jaccard(collect(m1.id), collect(m2.id)) AS jaccardSimilarity
RETURN round(1000 * jaccardSimilarity) / 1000 AS jaccardSimilarity
