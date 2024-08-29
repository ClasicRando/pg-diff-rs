SELECT
    JSON_OBJECT('schema_name': n.nspname, 'local_name': o.local_name) AS "name",
    JSON_OBJECT('oid': CAST(o.oid AS INTEGER), 'catalog': o.catalog) AS "dependency"
FROM (
    SELECT c.oid, 'pg_class' AS "catalog", c.relname AS local_name, c.relnamespace AS namespace_oid
    FROM pg_catalog.pg_class AS c
    WHERE c.relkind != 'c'
    UNION ALL
    SELECT t.oid, 'pg_type' AS "catalog", t.typname AS local_name, t.typnamespace AS namespace_oid
    FROM pg_catalog.pg_type AS t
    WHERE
        t.typtype IN ('e','r')
        OR
        (
            t.typtype = 'c'
            AND EXISTS(
                SELECT NULL
                FROM pg_catalog.pg_class tc
                WHERE
                    tc.oid = t.typrelid
                    AND tc.relkind = 'c'
            )
        )
    UNION ALL
    SELECT p.oid, 'pg_proc' AS "catalog", p.proname AS local_name, p.pronamespace AS namespace_oid
    FROM pg_catalog.pg_proc AS p
) AS o
JOIN pg_catalog.pg_namespace AS n
    ON o.namespace_oid = n.oid
WHERE
    n.nspname = ANY($1)
    AND o.local_name = $2;
