-- file:rowsecurity.sql ln:573 expect:true
SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '%28%'
