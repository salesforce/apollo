-- file:create_misc.sql ln:166 expect:true
INSERT INTO f_star (class, a, f)
   VALUES ('f', 26, '(11111,33333),(22222,44444)'::polygon)
