-- file:strings.sql ln:166 expect:true
SELECT regexp_matches(chr(10) || '1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4', '.?$', 'mg')
