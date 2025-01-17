ALTER POLICY test_policy
    ON test_schema.test_table
    TO PUBLIC
    USING (column IS NULL);