-- file:updatable_views.sql ln:979 expect:true
INSERT INTO t11
SELECT i,i,'t11','t11d' FROM generate_series(1,10) g(i)
