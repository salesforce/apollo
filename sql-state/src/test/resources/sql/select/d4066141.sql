-- file:case.sql ln:37 expect:true
SELECT '<NULL>' AS "One",
  CASE
    WHEN 1 > 2 THEN 3
  END AS "Simple default"
