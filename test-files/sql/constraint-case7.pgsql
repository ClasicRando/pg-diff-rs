ALTER TABLE test_schema.test_table ADD CONSTRAINT test_constraint
FOREIGN KEY (test_col) REFERENCES test_schema.ref_table(test_col) MATCH FULL
    ON DELETE CASCADE
    ON UPDATE NO ACTION
NOT DEFERRABLE;