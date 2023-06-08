MATCH (account:Account {id: $id})
RETURN account.createTime AS createTime, account.isBlocked AS isBlocked, account.type AS type