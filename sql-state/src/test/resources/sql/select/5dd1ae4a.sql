-- file:inet.sql ln:44 expect:true
SELECT '' AS four, c AS cidr, masklen(c) AS "masklen(cidr)",
  i AS inet, masklen(i) AS "masklen(inet)" FROM INET_TBL
  WHERE masklen(c) <= 8
