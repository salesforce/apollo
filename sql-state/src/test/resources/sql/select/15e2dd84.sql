-- file:updatable_views.sql ln:343 expect:true
SELECT table_name, is_insertable_into
  FROM information_schema.tables
 WHERE table_name LIKE 'rw_view%'
 ORDER BY table_name
