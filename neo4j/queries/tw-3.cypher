MATCH (dst:Account {id: $dstId}), (src:Account {id: $srcId})
CREATE (dst)-[:transfer {timestamp: $currentTime, amount: $amt}]-(src)
