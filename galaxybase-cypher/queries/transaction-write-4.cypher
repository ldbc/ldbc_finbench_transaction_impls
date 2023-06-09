MATCH (person:Person {id: $personId})
CREATE (person)-[:PersonOwnAccount]->(:Account {id: $accountId, createTime: $time, isBlocked: $accountBlocked, type: '$accountType'})
