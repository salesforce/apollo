-- file:horology.sql ln:93 expect:true
SELECT (timestamp without time zone 'today' = (timestamp without time zone 'tomorrow' - interval '1 day')) as "True"
