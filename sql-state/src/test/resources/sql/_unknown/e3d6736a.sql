-- file:aggregates.sql ln:209 expect:false
TRUE	null	FALSE	null
FALSE	TRUE	null	null
null	TRUE	FALSE	null
\.

SELECT
  BOOL_AND(b1)     AS "f",
  BOOL_AND(b2)     AS "t",
  BOOL_AND(b3)     AS "f",
  BOOL_AND(b4)     AS "n",
  BOOL_AND(NOT b2) AS "f",
  BOOL_AND(NOT b3) AS "t"
FROM bool_test
