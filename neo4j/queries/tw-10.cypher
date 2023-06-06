MATCH (acc:Account {id: $accountId})
SET acc.isBlocked = true
