-- file:int8.sql ln:105 expect:true
SELECT '' AS to_char_3, to_char( (q1 * -1), '9999999999999999PR'), to_char( (q2 * -1), '9999999999999999.999PR')
	FROM INT8_TBL
