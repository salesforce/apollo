-- file:privileges.sql ln:120 expect:true
SELECT * FROM atest1 WHERE ( b IN ( SELECT col1 FROM atest2 ) )
