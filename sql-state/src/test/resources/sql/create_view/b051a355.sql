-- file:updatable_views.sql ln:28 expect:true
CREATE VIEW ro_view18 AS SELECT * FROM (VALUES(1)) AS tmp(a)
