-- file:jsonb.sql ln:509 expect:true
CREATE DOMAIN jsb_int_array_2d  AS int[][] CHECK(array_length(VALUE, 2) = 3)
