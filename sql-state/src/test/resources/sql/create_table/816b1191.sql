-- file:foreign_key.sql ln:369 expect:true
CREATE TABLE PKTABLE ( ptest1 int, ptest2 int, ptest3 int, ptest4 text, PRIMARY KEY(ptest1, ptest2, ptest3) )
