-- file:foreign_data.sql ln:248 expect:true
ALTER USER MAPPING FOR current_user SERVER s5 OPTIONS (ADD modified '1')
