-- file:create_table.sql ln:685 expect:true
create or replace function func_part_create() returns trigger
  language plpgsql as $$
  begin
    execute 'create table tab_part_create_1 partition of tab_part_create for values in (1)'
