-- file:macaddr.sql ln:34 expect:true
SELECT b >= '08:00:2b:01:02:04' FROM macaddr_data WHERE a = 1
