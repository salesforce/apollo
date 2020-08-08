-- file:alter_generic.sql ln:84 expect:true
SELECT n.nspname, proname, prorettype::regtype, proisagg, a.rolname
  FROM pg_proc p, pg_namespace n, pg_authid a
  WHERE p.pronamespace = n.oid AND p.proowner = a.oid
    AND n.nspname IN ('alt_nsp1', 'alt_nsp2')
  ORDER BY nspname, proname
