MATCH (person:Person {id: $personId}), (company:Company {id: $companyId})
CREATE (person)-[:PersonInvestCompany {timestamp: $time, ratio: $ratio}]->(company)
