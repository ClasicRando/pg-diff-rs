SELECT
    n.nspname AS "name",
    au.rolname AS "owner"
FROM pg_catalog.pg_namespace n
JOIN pg_catalog.pg_authid au
    ON n.nspowner = au.oid
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname !~ '^pg_toast'
    AND n.nspname !~ '^pg_temp'
    -- Exclude schemas owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_namespace'::REGCLASS
            AND d.objid = n.oid
            AND d.deptype = 'e'
    );