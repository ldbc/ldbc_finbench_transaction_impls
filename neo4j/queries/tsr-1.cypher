MATCH (account:Account {id: $id})
RETURN account{.createTime,.isBlocked,.type}
