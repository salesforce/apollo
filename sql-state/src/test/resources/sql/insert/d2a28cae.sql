-- file:insert.sql ln:58 expect:true
insert into inserttest (f3.if1, f3.if2) select 3, '{baz,quux}'
