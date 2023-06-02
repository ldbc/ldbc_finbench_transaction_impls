CREATE (:Company {id: $companyId, name: $companyName})-[:own]->(:Account {id: $accountId, createTime: $currentTime, isBlocked: $accountBlocked, type: $accountType})
