-- file:timestamptz.sql ln:366 expect:true
SELECT '2011-03-27 04:00:00'::timestamp AT TIME ZONE 'Europe/Moscow'
