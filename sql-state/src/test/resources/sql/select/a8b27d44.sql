-- file:text.sql ln:77 expect:true
select format('%1$s %12$s', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
