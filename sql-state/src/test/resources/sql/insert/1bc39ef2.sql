-- file:triggers.sql ln:1781 expect:true
insert into my_table values (3, 'CCC'), (4, 'DDD')
  on conflict (a) do
  update set b = my_table.b || ':' || excluded.b
