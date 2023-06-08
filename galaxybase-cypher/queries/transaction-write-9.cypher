MATCH (company1:Company {id: $companyId1}), (company2:Company {id: $companyId2})
CREATE (company1)-[:CompanyInvestCompany {timestamp: $time, ratio: $ratio}]->(company2)

