-- file:rowsecurity.sql ln:681 expect:true
CREATE VIEW bv1 WITH (security_barrier) AS SELECT * FROM b1 WHERE a > 0 WITH CHECK OPTION
