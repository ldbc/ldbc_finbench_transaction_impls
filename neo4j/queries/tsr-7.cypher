MATCH (person:Person {id: $id})
OPTIONAL MATCH (person)-[edge1:invest]->(comp:Company)
RETURN collect(comp.id)
