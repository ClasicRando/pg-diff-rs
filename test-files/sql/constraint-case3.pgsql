ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
UNIQUE NULLS DISTINCT (test_col) DEFERRABLE INITIALLY IMMEDIATE;