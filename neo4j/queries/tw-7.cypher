MATCH (acc:Account {id: $accountId})
CREATE (acc)<-[:signIn {timestamp: $currentTime}]-(:Medium {id: $mediumId, isBlocked: $mediumBlocked})
