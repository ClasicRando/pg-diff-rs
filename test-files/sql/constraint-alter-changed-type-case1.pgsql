ALTER TABLE test_schema.test_table DROP CONSTRAINT test_constraint;
ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
CHECK(test_col2 = 'test') NO INHERIT NOT DEFERRABLE;