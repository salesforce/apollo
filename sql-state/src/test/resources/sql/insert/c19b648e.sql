-- file:insert_conflict.sql ln:345 expect:true
insert into capitals values ('Las Vegas', 5.83E+5, 2222, 'NV') on conflict (name) do update set population = excluded.population
