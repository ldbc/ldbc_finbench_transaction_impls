MATCH (med:Medium {id: $mediumId})
SET med.isBlocked = true
