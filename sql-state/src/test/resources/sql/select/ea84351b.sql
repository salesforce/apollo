-- file:timestamptz.sql ln:29 expect:true
SELECT count(*) AS One FROM TIMESTAMPTZ_TBL WHERE d1 = timestamp with time zone 'tomorrow'
