-- file:collate.icu.utf8.sql ln:329 expect:true
CREATE INDEX collate_test1_idx4 ON collate_test1 (((b||'foo') COLLATE "POSIX"))
