-- file:horology.sql ln:397 expect:true
SELECT to_timestamp('My birthday-> Year: 1976, Month: May, Day: 16',
                    '"My birthday-> Year" YYYY, "Month:" FMMonth, "Day:" DD')
