-- file:strings.sql ln:132 expect:true
SELECT regexp_replace('1112223333', E'(\\d{3})(\\d{3})(\\d{4})', E'(\\1) \\2-\\3')
