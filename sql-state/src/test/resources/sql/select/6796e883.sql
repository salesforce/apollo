-- file:timestamp.sql ln:29 expect:true
SELECT count(*) AS One FROM TIMESTAMP_TBL WHERE d1 = timestamp without time zone 'today'
