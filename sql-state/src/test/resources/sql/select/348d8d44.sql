-- file:oid.sql ln:41 expect:true
SELECT '' AS three, o.* FROM OID_TBL o WHERE o.f1 > '1234'
