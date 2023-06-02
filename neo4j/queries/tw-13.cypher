MATCH (p1:Person {id: $pid1}), (p2:Person {id: $pid2})
CREATE (p1)<-[:guarantee {timestamp: $currentTime}]-(p2)
