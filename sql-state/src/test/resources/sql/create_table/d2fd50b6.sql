-- file:inherit.sql ln:215 expect:true
create table c2 (constraint p2chk check (ff1 > 10) no inherit) inherits (p1)
