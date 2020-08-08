-- file:opr_sanity.sql ln:917 expect:true
SELECT a.aggfnoid::oid, p.proname, ptr.oid, ptr.proname
FROM pg_aggregate AS a, pg_proc AS p, pg_proc AS ptr
WHERE a.aggfnoid = p.oid AND
    a.aggminvtransfn = ptr.oid AND
    (ptr.proretset
     OR NOT (ptr.pronargs =
             CASE WHEN a.aggkind = 'n' THEN p.pronargs + 1
             ELSE greatest(p.pronargs - a.aggnumdirectargs, 1) + 1 END)
     OR NOT physically_coercible(ptr.prorettype, a.aggmtranstype)
     OR NOT physically_coercible(a.aggmtranstype, ptr.proargtypes[0])
     OR (p.pronargs > 0 AND
         NOT physically_coercible(p.proargtypes[0], ptr.proargtypes[1]))
     OR (p.pronargs > 1 AND
         NOT physically_coercible(p.proargtypes[1], ptr.proargtypes[2]))
     OR (p.pronargs > 2 AND
         NOT physically_coercible(p.proargtypes[2], ptr.proargtypes[3]))
         )
