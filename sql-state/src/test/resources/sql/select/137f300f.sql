-- file:horology.sql ln:83 expect:true
SELECT timestamp without time zone '1999-12-01' + interval '1 month - 1 second' AS "Dec 31"
