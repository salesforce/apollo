-- file:create_misc.sql ln:185 expect:true
INSERT INTO f_star (class, f)
   VALUES ('f', '(11111111,33333333),(22222222,44444444)'::polygon)
