-- file:timestamptz.sql ln:429 expect:true
SELECT '2011-03-27 00:00:00 UTC'::timestamptz AT TIME ZONE 'Europe/Moscow'
