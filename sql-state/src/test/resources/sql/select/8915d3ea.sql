-- file:case.sql ln:128 expect:true
SELECT *
  FROM CASE_TBL a, CASE2_TBL b
  WHERE COALESCE(a.f, b.i, b.j) = 2
