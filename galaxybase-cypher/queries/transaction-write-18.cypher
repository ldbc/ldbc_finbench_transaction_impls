MATCH (account:Account {id: $accountId})
SET account.isBlocked = true