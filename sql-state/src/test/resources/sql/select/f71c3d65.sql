-- file:arrays.sql ln:391 expect:true
select * from arr_tbl where f1 >= '{1,2,3}' and f1 < '{1,5,3}'
