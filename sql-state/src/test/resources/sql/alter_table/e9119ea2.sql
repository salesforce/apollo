-- file:inherit.sql ln:403 expect:true
alter table p1 add constraint inh_check_constraint2 check (f1 < 10)
