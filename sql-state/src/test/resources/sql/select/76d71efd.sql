-- file:cluster.sql ln:67 expect:true
SELECT a,b,c,substring(d for 30), length(d) from clstr_tst ORDER BY c
