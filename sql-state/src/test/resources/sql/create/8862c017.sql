-- file:insert_conflict.sql ln:272 expect:true
create unique index insertconflicti2 on insertconflict(b)
  where coalesce(a, 1) > 0
