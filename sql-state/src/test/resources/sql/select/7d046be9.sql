-- file:tsrf.sql ln:54 expect:true
SELECT few.dataa, count(*) FROM few WHERE dataa = 'a' GROUP BY few.dataa ORDER BY 2
