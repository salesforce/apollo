-- file:inet.sql ln:30 expect:true
INSERT INTO INET_TBL (c, i) VALUES (cidr('192.168.1.2/30'), '192.168.1.226')
