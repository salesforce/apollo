-- file:union.sql ln:31 expect:true
SELECT 1.1 AS two UNION ALL SELECT 2 ORDER BY 1
