MATCH (medium: Medium {id: $mediumId}), (account:Account {id: $accountId})
CREATE (medium)-[:MediumSignInAccount {timestamp: $time}]->(account)
