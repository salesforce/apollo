-- file:cluster.sql ln:66 expect:true
SELECT a,b,c,substring(d for 30), length(d) from clstr_tst ORDER BY b
