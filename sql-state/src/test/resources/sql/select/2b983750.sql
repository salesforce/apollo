-- file:jsonb.sql ln:751 expect:true
SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2
