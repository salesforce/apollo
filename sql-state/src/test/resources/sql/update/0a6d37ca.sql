-- file:returning.sql ln:15 expect:true
UPDATE foo SET f2 = lower(f2), f3 = DEFAULT RETURNING foo.*, f1+f3 AS sum13
