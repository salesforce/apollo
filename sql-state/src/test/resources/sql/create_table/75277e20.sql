-- file:create_index.sql ln:103 expect:true
CREATE TABLE quad_point_tbl AS
    SELECT point(unique1,unique2) AS p FROM tenk1
