-- file:aggregates.sql ln:536 expect:true
select max(foo COLLATE "C") filter (where (bar collate "POSIX") > '0')
from (values ('a', 'b')) AS v(foo,bar)
