-- file:plpgsql.sql ln:1978 expect:true
select namedparmcursor_test1(20000, 20000) as "Should be false",
       namedparmcursor_test1(20, 20) as "Should be true"
