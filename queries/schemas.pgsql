SELECT n.nspname AS "schema_name"
FROM pg_catalog.pg_namespace n
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname !~ '^pg_toast'
    AND n.nspname !~ '^pg_temp'
    -- Exclude schemas owned by extensions
    AND NOT EXISTS (
        SELECT d.objid
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_namespace'::REGCLASS
            AND d.objid = n.oid
            AND d.deptype = 'e'
    );