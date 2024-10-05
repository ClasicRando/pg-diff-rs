ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
CHECK(test_col = 'test' AND test_col = test_col2) NOT DEFERRABLE;