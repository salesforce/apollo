-- file:cluster.sql ln:60 expect:true
INSERT INTO clstr_tst (b, c, d) VALUES (6, 'seis', repeat('xyzzy', 100000))
