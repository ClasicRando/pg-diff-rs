DROP POLICY test_policy ON test_schema.test_table;
CREATE POLICY test_policy
    ON test_schema.test_table
    AS RESTRICTIVE
    FOR INSERT
    TO PUBLIC
    WITH CHECK (column IS NULL);