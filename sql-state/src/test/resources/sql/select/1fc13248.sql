-- file:timestamptz.sql ln:380 expect:true
SELECT '2014-10-26 01:00:00'::timestamp AT TIME ZONE 'Europe/Moscow'
