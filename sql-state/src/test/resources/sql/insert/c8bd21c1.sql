-- file:create_misc.sql ln:126 expect:true
INSERT INTO e_star (class, a, c)
   VALUES ('e', 16, 'hi bob'::name)
