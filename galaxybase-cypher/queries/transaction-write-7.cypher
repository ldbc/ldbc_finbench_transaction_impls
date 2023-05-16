MATCH (acc:Account {id: '$accountId'})
CREATE (acc)<-[:MediumSignInAccount {timestamp: $time}]-(:Medium {id: '$mediumId', isBlocked: $mediumBlocked})
