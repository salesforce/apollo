-- file:inherit.sql ln:551 expect:true
explain (costs off)
SELECT min(x) FROM
  (SELECT unique1 AS x FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x FROM tenk1 b) s
