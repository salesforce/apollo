-- file:horology.sql ln:139 expect:true
SELECT '' AS "64", d1 + interval '1 year' AS one_year FROM TIMESTAMPTZ_TBL
