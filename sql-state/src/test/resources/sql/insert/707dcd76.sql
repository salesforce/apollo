-- file:create_misc.sql ln:92 expect:true
INSERT INTO d_star (class, b, c, d)
   VALUES ('d', 'crumble'::text, 'hi boris'::name, '100.001'::float8)
