ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
FOREIGN KEY (test_col,test_col2) REFERENCES test_schema.ref_table(test_col,test_col2) MATCH PARTIAL
    ON DELETE SET NULL (test_col)
    ON UPDATE SET DEFAULT (test_col)
NOT DEFERRABLE;