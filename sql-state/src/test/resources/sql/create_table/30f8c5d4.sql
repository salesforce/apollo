-- file:foreign_key.sql ln:481 expect:true
CREATE TABLE PKTABLE (ptest1 int, ptest2 inet, PRIMARY KEY(ptest1, ptest2))
