-- file:rowsecurity.sql ln:497 expect:true
CREATE POLICY r2 ON rec2 USING (a = (SELECT x FROM rec1 WHERE y = b))
