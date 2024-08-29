SELECT
    JSON_OBJECT('schema_name': pn.nspname, 'local_name': t.relname) AS "name",
    JSON_OBJECT('oid': CAST(t.oid AS INTEGER), 'catalog': 'pg_class') AS "dependency"
FROM pg_catalog.pg_class AS t
JOIN pg_catalog.pg_namespace AS pn
    ON t.relnamespace = pn.oid
WHERE
    pn.nspname = ANY($1)
    AND t.relname = $2
    AND t.relkind != 'c'
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );
