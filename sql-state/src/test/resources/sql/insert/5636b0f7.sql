-- file:rangetypes.sql ln:331 expect:true
insert into test_range_excl
  values(int4range(124, 124, '[]'), int4range(3, 3, '[]'), '[2010-01-02 10:10, 2010-01-02 11:10)')
