-- file:returning.sql ln:66 expect:true
UPDATE foo SET f4 = f4 + f3 WHERE f4 = 99 RETURNING *
