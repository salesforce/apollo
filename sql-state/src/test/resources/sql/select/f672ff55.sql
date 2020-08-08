-- file:opr_sanity.sql ln:846 expect:true
SELECT a.aggfnoid::oid, p.proname, pfn.oid, pfn.proname
FROM pg_aggregate AS a, pg_proc AS p, pg_proc AS pfn
WHERE a.aggfnoid = p.oid AND
    a.aggfinalfn = pfn.oid AND
    (pfn.proretset OR
     NOT binary_coercible(pfn.prorettype, p.prorettype) OR
     NOT binary_coercible(a.aggtranstype, pfn.proargtypes[0]) OR
     CASE WHEN a.aggfinalextra THEN pfn.pronargs != p.pronargs + 1
          ELSE pfn.pronargs != a.aggnumdirectargs + 1 END
     OR (pfn.pronargs > 1 AND
         NOT binary_coercible(p.proargtypes[0], pfn.proargtypes[1]))
     OR (pfn.pronargs > 2 AND
         NOT binary_coercible(p.proargtypes[1], pfn.proargtypes[2]))
     OR (pfn.pronargs > 3 AND
         NOT binary_coercible(p.proargtypes[2], pfn.proargtypes[3]))
         )
