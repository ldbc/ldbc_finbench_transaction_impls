MATCH (company:Company {id: $companyId})
CREATE (company)-[:CompanyOwnAccount]->(:Account {id: $accountId, createTime: $time, isBlocked: $accountBlocked, type: '$accountType'})
