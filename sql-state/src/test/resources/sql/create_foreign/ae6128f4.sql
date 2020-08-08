-- file:foreign_data.sql ln:280 expect:true
CREATE FOREIGN TABLE ft1 (
	c1 integer OPTIONS ("param 1" 'val1') REFERENCES ref_table (id),
	c2 text OPTIONS (param2 'val2', param3 'val3'),
	c3 date
) SERVER s0 OPTIONS (delimiter ',', quote '"', "be quoted" 'value')
