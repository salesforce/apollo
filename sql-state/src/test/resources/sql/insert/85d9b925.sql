-- file:cluster.sql ln:24 expect:true
INSERT INTO clstr_tst_s (b) SELECT b FROM clstr_tst_s
