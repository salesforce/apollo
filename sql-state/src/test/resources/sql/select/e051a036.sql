-- file:timestamp.sql ln:164 expect:true
SELECT '' AS "16", d1 FROM TIMESTAMP_TBL
   WHERE d1 <= timestamp without time zone '1997-01-02'
