-- file:inet.sql ln:69 expect:true
SELECT * FROM inet_tbl WHERE i<<='192.168.1.0/24'::cidr
