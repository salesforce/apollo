-- file:collate.linux.utf8.sql ln:95 expect:true
SELECT 'bbc' COLLATE "en_US" > 'äbc' COLLATE "en_US" AS "true"
