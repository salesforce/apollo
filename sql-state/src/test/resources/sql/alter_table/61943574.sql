-- file:alter_table.sql ln:335 expect:true
alter table parent_noinh_convalid add constraint check_a_is_2 check (a = 2) no inherit not valid
