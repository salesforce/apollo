-- file:timestamptz.sql ln:185 expect:true
SELECT '' AS "16", d1 FROM TIMESTAMPTZ_TBL
   WHERE d1 <= timestamp with time zone '1997-01-02'
