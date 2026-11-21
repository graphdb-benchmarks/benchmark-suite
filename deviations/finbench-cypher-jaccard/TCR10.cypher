MATCH (p:Person)-[e:invest]->(c:Company)
WHERE p.id IN [$id1, $id2]
  AND $start_time < e.timestamp AND e.timestamp < $end_time
WITH c.id AS companyId,
     max(CASE WHEN p.id = $id1 THEN 1 ELSE 0 END) AS belongsToFirst,
     max(CASE WHEN p.id = $id2 THEN 1 ELSE 0 END) AS belongsToSecond
WITH count(*) AS unionSize,
     sum(CASE WHEN belongsToFirst = 1 AND belongsToSecond = 1 THEN 1 ELSE 0 END) AS intersectionSize
RETURN CASE WHEN unionSize = 0 THEN 0.0
            ELSE floor(1000.0 * intersectionSize / unionSize + 0.5) / 1000.0
       END AS jaccardSimilarity
