-- file:create_misc.sql ln:98 expect:true
INSERT INTO d_star (class, a, c)
   VALUES ('d', 12, 'hi avi'::name)
