-- file:horology.sql ln:74 expect:true
SELECT date '1991-02-03' - time with time zone '04:05:06 UTC' AS "Subtract Time UTC"
