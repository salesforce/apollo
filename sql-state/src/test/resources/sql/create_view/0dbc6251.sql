-- file:updatable_views.sql ln:722 expect:true
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl WHERE a = ANY (b)
  WITH CHECK OPTION
