-- file:horology.sql ln:226 expect:true
SELECT (time '00:00', interval '1 hour')
  OVERLAPS (time '00:30', interval '1 hour') AS "True"
