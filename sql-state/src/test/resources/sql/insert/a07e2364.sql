-- file:privileges.sql ln:318 expect:true
INSERT INTO atest5(two) VALUES (6) ON CONFLICT (two) DO UPDATE set three = EXCLUDED.one
