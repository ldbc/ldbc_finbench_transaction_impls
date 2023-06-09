MATCH (person1:Person {id: $personId1}), (person2:Person {id: $personId2})
CREATE (person1)-[:PersonGuaranteePerson {timestamp: $time}]->(person2)