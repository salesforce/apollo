-- file:foreign_key.sql ln:400 expect:true
UPDATE PKTABLE set ptest1=0, ptest2=-1, ptest3=-2 where ptest2=2
