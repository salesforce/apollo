-- file:updatable_views.sql ln:572 expect:true
UPDATE rw_view2 SET base_a = 1.05 WHERE base_a = 1.1
