-- file:pg_lsn.sql ln:21 expect:true
SELECT '0/16AE7F8'::pg_lsn != '0/16AE7F7'
