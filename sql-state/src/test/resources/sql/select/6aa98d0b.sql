-- file:inherit.sql ln:154 expect:true
select tableoid::regclass::text as relname, parted_tab.* from parted_tab order by 1,2
