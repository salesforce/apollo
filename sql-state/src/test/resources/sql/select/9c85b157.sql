-- file:horology.sql ln:121 expect:true
SELECT (timestamp with time zone 'tomorrow' = (timestamp with time zone 'yesterday' + interval '2 days')) as "True"
