-- file:create_index.sql ln:106 expect:true
INSERT INTO quad_point_tbl
    SELECT '(333.0,400.0)'::point FROM generate_series(1,1000)
