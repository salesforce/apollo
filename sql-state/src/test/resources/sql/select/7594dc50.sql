-- file:union.sql ln:15 expect:true
SELECT 1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1
