ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
FOREIGN KEY (test_col,test_col2) REFERENCES test_schema.ref_table(test_col,test_col2) MATCH SIMPLE
    ON DELETE RESTRICT
    ON UPDATE SET DEFAULT
NOT DEFERRABLE;