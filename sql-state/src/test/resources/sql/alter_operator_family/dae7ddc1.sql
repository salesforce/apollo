-- file:equivclass.sql ln:84 expect:true
alter operator family integer_ops using btree add
  operator 3 = (int8alias1, int8alias2)
