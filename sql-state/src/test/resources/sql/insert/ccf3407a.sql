-- file:insert_conflict.sql ln:294 expect:true
insert into insertconflictv values (1,'foo')
  on conflict (f1) do update set f2 = excluded.f2
