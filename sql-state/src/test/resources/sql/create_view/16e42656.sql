-- file:updatable_views.sql ln:1078 expect:true
CREATE VIEW v1 WITH (security_barrier = true) AS
  SELECT * FROM t1 WHERE (a > 0)
  WITH CHECK OPTION
