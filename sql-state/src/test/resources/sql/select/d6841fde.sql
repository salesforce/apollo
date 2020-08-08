-- file:opr_sanity.sql ln:1282 expect:true
SELECT p1.indexrelid, p1.indrelid
FROM pg_index as p1
WHERE array_lower(indkey, 1) != 0 OR array_upper(indkey, 1) != indnatts-1 OR
    array_lower(indclass, 1) != 0 OR array_upper(indclass, 1) != indnatts-1 OR
    array_lower(indcollation, 1) != 0 OR array_upper(indcollation, 1) != indnatts-1 OR
    array_lower(indoption, 1) != 0 OR array_upper(indoption, 1) != indnatts-1
