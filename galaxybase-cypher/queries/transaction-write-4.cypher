MATCH (dst:Account {id: '$dstId'}), (src:Account {id: '$srcId'})
CREATE (dst)<-[:AccountWithdrawAccount {timestamp: $time, amount: $amount}]-(src)
