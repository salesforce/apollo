-- file:rowsecurity.sql ln:395 expect:true
CREATE POLICY pp3 ON part_document_satire AS RESTRICTIVE
    USING (cid < 55)
