MATCH (p1:Person {id: '$pid1'}), (p2:Person {id: '$pid2'})
CREATE (p1)<-[:PersonGuaranteePerson {timestamp: $time}]-(p2)
