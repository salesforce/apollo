-- file:updatable_views.sql ln:21 expect:true
CREATE VIEW ro_view11 AS SELECT b1.a, b2.b FROM base_tbl b1, base_tbl b2
