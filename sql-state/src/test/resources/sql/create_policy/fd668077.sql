-- file:rowsecurity.sql ln:546 expect:true
CREATE POLICY p1 ON s1 USING (a in (select x from s2 where y like '%2f%'))
