-- file:equivclass.sql ln:96 expect:true
create function int8alias1cmp(int8, int8alias1) returns int
  strict immutable language internal as 'btint8cmp'
