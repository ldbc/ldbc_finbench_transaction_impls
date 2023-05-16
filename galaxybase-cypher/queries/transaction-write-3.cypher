MATCH (dst:Account {id: '$dstId'}), (src:Account {id: '$srcId'})
CREATE (dst)<-[:AccountTransferAccount {timestamp: $time, amount: $amount}]-(src)
