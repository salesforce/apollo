-- file:triggers.sql ln:307 expect:true
SELECT pg_get_triggerdef(oid, false) FROM pg_trigger WHERE tgrelid = 'main_table'::regclass AND tgname = 'modified_a'
