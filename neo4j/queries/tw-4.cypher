MATCH (dst:Account {id: $dstId, type: 'card'}), (src:Account {id: $srcId})
CREATE (dst)-[:withdraw {timestamp: $currentTime, amount: $amt}]-(src)
