-- file:union.sql ln:35 expect:true
SELECT 1.1 AS three UNION SELECT 2 UNION SELECT 3 ORDER BY 1
