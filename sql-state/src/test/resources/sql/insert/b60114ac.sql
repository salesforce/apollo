-- file:insert.sql ln:61 expect:true
insert into inserttest (f3.if2[1], f3.if2[2]) values ('foo', 'bar')
