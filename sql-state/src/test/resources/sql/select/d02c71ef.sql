-- file:privileges.sql ln:625 expect:true
select has_table_privilege(current_user,t1.oid,'references')
from (select oid from pg_class where relname = 'pg_authid') as t1
