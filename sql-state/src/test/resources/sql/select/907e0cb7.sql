-- file:money.sql ln:109 expect:true
SELECT '90000000000000099.00'::money / 10::int
