-- file:tsearch.sql ln:276 expect:false
Water, water, every where,
  Nor any drop to drink.
S. T. Coleridge (1772-1834)
', to_tsquery('english', 'paint&water'))
