-- file:equivclass.sql ln:65 expect:true
create function int8alias1eq(int8, int8alias1) returns bool
  strict immutable language internal as 'int8eq'
