-- file:tsearch.sql ln:21 expect:true
SELECT oid, tmplname
FROM pg_ts_template
WHERE tmplnamespace = 0 OR tmpllexize = 0
