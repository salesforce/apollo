-- file:rowsecurity.sql ln:103 expect:true
CREATE POLICY p2r ON document AS RESTRICTIVE TO regress_rls_dave
    USING (cid <> 44 AND cid < 50)
