-- file:rowtypes.sql ln:61 expect:true
insert into quadtable (f1, q.c1.r, q.c2.i) values(44,55,66)
