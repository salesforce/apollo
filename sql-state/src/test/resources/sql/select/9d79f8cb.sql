-- file:timestamptz.sql ln:425 expect:true
SELECT '2011-03-26 22:59:59 UTC'::timestamptz AT TIME ZONE 'Europe/Moscow'
