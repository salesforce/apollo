-- file:create_misc.sql ln:89 expect:true
INSERT INTO d_star (class, a, c, d)
   VALUES ('d', 10, 'hi kristin'::name, '10.01'::float8)
