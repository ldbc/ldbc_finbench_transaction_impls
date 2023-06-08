MATCH (person:Person {id: $personId})
SET person.isBlocked = true