CREATE (:Person {id: $personId, name: $personName})-[:own]->(:Account {id: $accountId, createTime: $currentTime, isBlocked: $accountBlocked, type: $accountType})
