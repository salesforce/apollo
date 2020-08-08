-- file:inherit.sql ln:207 expect:true
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.connoinherit from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname = 'p1' order by 1,2
