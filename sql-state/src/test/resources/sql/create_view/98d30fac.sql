-- file:updatable_views.sql ln:1082 expect:true
CREATE VIEW v2 WITH (security_barrier = true) AS
  SELECT * FROM v1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.cc = v1.c)
  WITH CHECK OPTION
