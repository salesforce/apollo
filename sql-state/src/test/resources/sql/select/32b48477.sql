-- file:privileges.sql ln:867 expect:true
SELECT lo_truncate(lo_open(1002, x'20000'::int), 10)
