ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
CHECK(test_col = 'test') NO INHERIT NOT DEFERRABLE;