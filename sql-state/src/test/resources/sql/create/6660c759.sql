-- file:returning.sql ln:115 expect:true
CREATE OR REPLACE RULE voo_u AS ON UPDATE TO voo DO INSTEAD
  UPDATE foo SET f1 = new.f1, f2 = new.f2 WHERE f1 = old.f1
  RETURNING f1, f2
