-- file:rowsecurity.sql ln:267 expect:true
CREATE POLICY p2 ON t2 FOR ALL TO PUBLIC USING (a % 2 = 1)
