-- file:tsearch.sql ln:12 expect:true
SELECT oid, prsname
FROM pg_ts_parser
WHERE prsnamespace = 0 OR prsstart = 0 OR prstoken = 0 OR prsend = 0 OR
            prslextype = 0
