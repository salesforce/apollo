-- file:opr_sanity.sql ln:1158 expect:true
SELECT p1.oid, p1.amname, p2.oid, p2.proname
FROM pg_am AS p1, pg_proc AS p2
WHERE p2.oid = p1.amhandler AND
    (p2.prorettype != 'index_am_handler'::regtype OR p2.proretset
     OR p2.pronargs != 1
     OR p2.proargtypes[0] != 'internal'::regtype)
