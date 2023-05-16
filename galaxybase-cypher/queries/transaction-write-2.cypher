CREATE (:Company {id: '$companyId', name: '$companyName'})-[:CompanyOwnAccount]->(:Account {id: '$accountId', createTime: $time, isBlocked: $accountBlocked, type: '$accountType'})
