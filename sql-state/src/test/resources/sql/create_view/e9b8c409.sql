-- file:updatable_views.sql ln:744 expect:true
CREATE VIEW rw_view1 AS
  SELECT * FROM base_tbl b
  WHERE EXISTS(SELECT 1 FROM ref_tbl r WHERE r.a = b.a)
  WITH CHECK OPTION
