-- file:foreign_key.sql ln:599 expect:true
create table fktable(ftest1 int, ftest2 inet, foreign key(ftest1, ftest2) references pktable(ptest1, base1))
