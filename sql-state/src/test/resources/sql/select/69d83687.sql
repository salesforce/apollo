-- file:horology.sql ln:525 expect:true
SELECT to_char('2012-12-12 12:00'::timestamptz, 'YYYY-MM-DD HH:MI:SS TZ')
