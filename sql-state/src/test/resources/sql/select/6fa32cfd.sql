-- file:macaddr.sql ln:37 expect:true
SELECT b <> '08:00:2b:01:02:03' FROM macaddr_data WHERE a = 1
