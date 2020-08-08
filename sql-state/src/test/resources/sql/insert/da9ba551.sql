-- file:triggers.sql ln:1771 expect:true
insert into my_table values (1, 'AAA'), (2, 'BBB')
  on conflict (a) do
  update set b = my_table.b || ':' || excluded.b
