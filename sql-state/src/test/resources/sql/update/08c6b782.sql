-- file:returning.sql ln:152 expect:true
UPDATE joinview SET f1 = f1 + 1 WHERE f3 = 57 RETURNING *, other + 1
