-- file:macaddr8.sql ln:78 expect:true
SELECT b >  '08:00:2b:01:02:03:04:05' FROM macaddr8_data WHERE a = 15
