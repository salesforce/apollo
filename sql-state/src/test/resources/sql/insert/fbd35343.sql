-- file:updatable_views.sql ln:985 expect:true
INSERT INTO t12
SELECT i,i,'t12','{1,2}'::int[] FROM generate_series(1,10) g(i)
