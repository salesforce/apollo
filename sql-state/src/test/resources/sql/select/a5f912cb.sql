-- file:privileges.sql ln:844 expect:true
SELECT loread(lo_open(1005, x'40000'::int), 32)
