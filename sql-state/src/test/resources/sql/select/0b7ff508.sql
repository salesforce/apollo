-- file:case.sql ln:62 expect:true
SELECT '7' AS "None",
   CASE WHEN random() < 0 THEN 1
   END AS "NULL on no matches"
