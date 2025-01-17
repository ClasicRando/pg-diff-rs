DROP POLICY test_policy ON test_schema.test_table;
CREATE POLICY test_policy
    ON test_schema.test_table
    AS PERMISSIVE
    FOR UPDATE
    TO PUBLIC
    USING (column IS NULL);