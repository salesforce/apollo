-- file:horology.sql ln:235 expect:true
SELECT (time '00:00', interval '1 hour')
  OVERLAPS (time '01:30', interval '1 day') AS "False"
