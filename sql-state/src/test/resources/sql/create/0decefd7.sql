-- file:alter_table.sql ln:427 expect:true
CREATE TEMP TABLE PKTABLE (ptest1 int, ptest2 inet,
                           PRIMARY KEY(ptest1, ptest2))
