-- file:rowsecurity.sql ln:693 expect:true
EXPLAIN (COSTS OFF) UPDATE bv1 SET b = 'yyy' WHERE a = 4 AND f_leak(b)
