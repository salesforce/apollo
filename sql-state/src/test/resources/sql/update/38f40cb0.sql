-- file:updatable_views.sql ln:1005 expect:true
UPDATE v1 SET a=100 WHERE snoop(a) AND leakproof(a) AND a < 7 AND a != 6
