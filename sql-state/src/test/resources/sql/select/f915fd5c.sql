-- file:timestamptz.sql ln:358 expect:true
SELECT '2011-03-27 00:00:00'::timestamp AT TIME ZONE 'Europe/Moscow'
