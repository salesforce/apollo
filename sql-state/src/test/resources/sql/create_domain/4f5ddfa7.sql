-- file:domain.sql ln:363 expect:true
create domain dtop dinter check (substring(VALUE, 2, 1) = '1')
