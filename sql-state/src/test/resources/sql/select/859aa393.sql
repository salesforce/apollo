-- file:create_index.sql ln:487 expect:true
SELECT count(*) FROM quad_point_tbl WHERE p >> '(5000, 4000)'
