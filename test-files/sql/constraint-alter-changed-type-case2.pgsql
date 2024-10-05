ALTER TABLE test_schema.test_table DROP CONSTRAINT test_constraint;
ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
UNIQUE NULLS NOT DISTINCT (test_col) NOT DEFERRABLE;