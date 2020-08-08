-- file:updatable_views.sql ln:995 expect:true
CREATE VIEW v1 WITH (security_barrier=true) AS
SELECT *, (SELECT d FROM t11 WHERE t11.a = t1.a LIMIT 1) AS d
FROM t1
WHERE a > 5 AND EXISTS(SELECT 1 FROM t12 WHERE t12.a = t1.a)
