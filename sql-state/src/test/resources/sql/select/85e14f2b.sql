-- file:macaddr8.sql ln:73 expect:true
SELECT b::macaddr <> '08:00:2b:01:02:04'::macaddr FROM macaddr8_data WHERE a = 1
