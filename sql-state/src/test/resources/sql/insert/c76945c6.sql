-- file:updatable_views.sql ln:1188 expect:true
insert into uv_iocu_view (a, b) values ('xyxyxy', 3)
   on conflict (a) do update set b = cast(excluded.two as float)
