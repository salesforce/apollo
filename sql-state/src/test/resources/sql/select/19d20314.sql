-- file:privileges.sql ln:616 expect:true
select has_table_privilege(t2.oid,'pg_authid','update')
from (select oid from pg_roles where rolname = current_user) as t2
