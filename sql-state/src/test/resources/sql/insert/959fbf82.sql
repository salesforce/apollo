-- file:point.sql ln:86 expect:true
INSERT INTO point_gist_tbl SELECT '(0,0)' FROM generate_series(0,1000)
