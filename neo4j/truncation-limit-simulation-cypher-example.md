
Load the test data set into the database.

```
MATCH (n)
DETACH DELETE n;

CREATE
  (a1:Account {id: 1}),
  (a2:Account {id: 2}),
  (a3:Account {id: 3}),
  (a4:Account {id: 4}),
  (a5:Account {id: 5}),
  (a1)-[:transfer {amount: 20}]->(a2),
  (a1)-[:transfer {amount: 30}]->(a3),
  (a1)-[:transfer {amount: 40}]->(a4),
  (a1)-[:transfer {amount: 50}]->(a5)
;
```

Precompute the cutoff values for each potential `truncationLimit` (1..`$maxTruncationLimit`) value and save it in the `cutoffs` property.

```
:param maxTruncationLimit: 5
```
```
MATCH (a:Account)-[t:transfer]->(:Account)
WITH a, t
ORDER BY a.id ASC, t.amount DESC
WITH a, collect(t.amount) AS amounts
// precompute cutoffs for potential values of truncationLimit between 1 and $maxTruncationLimit
UNWIND range(1, $maxTruncationLimit) AS truncationLimit
WITH
  a, amounts,
  // if there are fewer edges than the truncationLimit, the cutoff should be at -1
  collect(coalesce(amounts[truncationLimit - 1], -1)) AS cutoffs
SET a.cutoffs = cutoffs;
```

Use the truncation during traversal.

```
:param truncationLimit: 4
```
```
MATCH p=(a:Account {id: 1})-[t:transfer*1..3]->(:Account)
WHERE all(e IN relationships(p) WHERE startNode(e).cutoffs[$truncationLimit] < e.amount)
RETURN p
```
