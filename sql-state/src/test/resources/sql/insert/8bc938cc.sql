-- file:domain.sql ln:133 expect:true
insert into dcomptable (d1[1]) values(row(9,10))
