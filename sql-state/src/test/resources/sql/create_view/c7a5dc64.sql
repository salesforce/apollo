-- file:updatable_views.sql ln:11 expect:true
CREATE VIEW ro_view1 AS SELECT DISTINCT a, b FROM base_tbl
