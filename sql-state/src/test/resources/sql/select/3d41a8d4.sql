-- file:union.sql ln:27 expect:true
SELECT 1 AS two UNION SELECT 2.2 ORDER BY 1
