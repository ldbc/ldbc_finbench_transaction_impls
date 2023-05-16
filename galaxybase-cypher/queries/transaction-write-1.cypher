CREATE (:Person {id: '$personId', name: '$personName'})-[:PersonOwnAccount]->(:Account {id: '$accountId', createTime: $time, isBlocked: $accountBlocked, type: '$accountType'})
