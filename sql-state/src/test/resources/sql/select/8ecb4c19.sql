-- file:timestamp.sql ln:44 expect:true
SELECT count(*) AS two FROM TIMESTAMP_TBL WHERE d1 = timestamp(2) without time zone 'now'
