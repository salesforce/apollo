-- file:updatable_views.sql ln:74 expect:true
INSERT INTO rw_view15 (a) VALUES (3) ON CONFLICT (a) DO NOTHING
