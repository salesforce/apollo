-- file:join.sql ln:507 expect:true
select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol
