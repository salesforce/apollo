-- file:rowsecurity.sql ln:92 expect:true
CREATE POLICY p1 ON document AS PERMISSIVE
    USING (dlevel <= (SELECT seclv FROM uaccount WHERE pguser = current_user))
