-- file:timestamptz.sql ln:435 expect:true
SELECT '2014-10-25 23:00:00 UTC'::timestamptz AT TIME ZONE 'Europe/Moscow'
