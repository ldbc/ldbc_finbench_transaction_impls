MATCH path=(comp:Company {id: $id})<-[:invest*1..3]-(investor)
WHERE (investor:Company) OR (investor:Person)
WITH
  investor.id AS id,
  labels(investor)[0] AS type,
  reduce(
    // We traverse along the investment path.
    // For each edge, we take their start node
    // (determined from the perspective of the path,
    // not based on direction, i.e. here it is
    // (startNode)<-[:transfer]-(endNode)).
    ratio = 1.0, e IN relationships(path) |
      ratio * e.amount / apoc.coll.sum( [ (:Company {id: startNode(e).id})<-[inv:invest]-() | inv.amount ] )
  ) AS ratio
RETURN id, type, round(1000 * ratio) / 1000 AS ratio
ORDER BY ratio DESC
