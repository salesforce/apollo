-- file:rangefuncs.sql ln:299 expect:true
SELECT * FROM generate_series(1,2) r1, generate_series(r1,3) r2, ROWS FROM( foo_sql(10+r1,13), foo_mat(10+r2,13) )
