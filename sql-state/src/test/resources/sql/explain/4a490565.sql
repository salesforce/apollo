-- file:create_index.sql ln:461 expect:true
EXPLAIN (COSTS OFF)
SELECT count(*) FROM quad_point_tbl WHERE p IS NULL
