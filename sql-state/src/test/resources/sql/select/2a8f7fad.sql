-- file:opr_sanity.sql ln:138 expect:true
SELECT p1.oid, p1.proname, p2.oid, p2.proname
FROM pg_proc AS p1, pg_proc AS p2
WHERE p1.oid < p2.oid AND
    p1.prosrc = p2.prosrc AND
    p1.prolang = 12 AND p2.prolang = 12 AND
    (p1.proisagg = false OR p2.proisagg = false) AND
    (p1.prolang != p2.prolang OR
     p1.proisagg != p2.proisagg OR
     p1.prosecdef != p2.prosecdef OR
     p1.proleakproof != p2.proleakproof OR
     p1.proisstrict != p2.proisstrict OR
     p1.proretset != p2.proretset OR
     p1.provolatile != p2.provolatile OR
     p1.pronargs != p2.pronargs)
