ALTER POLICY test_policy
    ON test_schema.test_table
    TO PUBLIC
    WITH CHECK (column IS NULL);