-- file:join.sql ln:509 expect:true
select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol
