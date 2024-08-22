SELECT
    e.extname AS "name",
    e.extversion AS "version",
    n.nspname AS "schema_name",
    e.extrelocatable AS is_relocatable
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n
    ON e.extnamespace = n.oid
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname !~ '^pg_toast'
    AND n.nspname !~ '^pg_temp';