-- file:updatable_views.sql ln:573 expect:true
DELETE FROM rw_view2 WHERE base_a = 1.05 RETURNING base_a, s, c, t
