-- file:window.sql ln:523 expect:true
SELECT i,SUM(v::int) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
  FROM (VALUES(1,1),(2,2),(3,NULL),(4,NULL)) t(i,v)
