-- file:union.sql ln:33 expect:true
SELECT 1.0::float8 AS two UNION ALL SELECT 1 ORDER BY 1
