-- file:aggregates.sql ln:185 expect:true
SELECT
      boolor_statefunc(NULL, NULL)  IS NULL AS "t",
  boolor_statefunc(TRUE, NULL)  IS NULL AS "t",
  boolor_statefunc(FALSE, NULL) IS NULL AS "t",
  boolor_statefunc(NULL, TRUE)  IS NULL AS "t",
  boolor_statefunc(NULL, FALSE) IS NULL AS "t",
    boolor_statefunc(TRUE, TRUE) AS "t",
  boolor_statefunc(TRUE, FALSE) AS "t",
  boolor_statefunc(FALSE, TRUE) AS "t",
  NOT boolor_statefunc(FALSE, FALSE) AS "t"
