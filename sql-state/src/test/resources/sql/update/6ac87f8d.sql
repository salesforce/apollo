-- file:arrays.sql ln:161 expect:true
UPDATE point_tbl SET f1[0] = NULL WHERE f1::text = '(10,10)'::point::text RETURNING *
