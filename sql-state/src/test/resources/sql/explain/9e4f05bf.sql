-- file:equivclass.sql ln:180 expect:true
explain (costs off)
  select * from ec1,
    (select ff + 1 as x from
       (select ff + 2 as ff from ec1
        union all
        select ff + 3 as ff from ec1) ss0
     union all
     select ff + 4 as x from ec1) as ss1,
    (select ff + 1 as x from
       (select ff + 2 as ff from ec1
        union all
        select ff + 3 as ff from ec1) ss0
     union all
     select ff + 4 as x from ec1) as ss2
  where ss1.x = ec1.f1 and ss1.x = ss2.x and ec1.ff = 42::int8
