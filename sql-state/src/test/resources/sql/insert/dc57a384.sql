-- file:sequence.sql ln:95 expect:true
INSERT INTO serialTest2 (f1, f6)
  VALUES ('bogus', -9223372036854775809)
