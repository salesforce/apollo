-- file:insert_conflict.sql ln:352 expect:true
insert into cities values ('Las Vegas', 5.86E+5, 2223) on conflict (name) do update set population = excluded.population, altitude = excluded.altitude
