-- file:collate.icu.utf8.sql ln:334 expect:true
SELECT relname, pg_get_indexdef(oid) FROM pg_class WHERE relname LIKE 'collate_test%_idx%' ORDER BY 1
