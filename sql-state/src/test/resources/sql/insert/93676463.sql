-- file:sequence.sql ln:98 expect:true
INSERT INTO serialTest2 (f1, f5)
  VALUES ('bogus', 9223372036854775808)
