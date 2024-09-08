SELECT JSON_OBJECT('schema_name': pn.nspname, 'local_name': p.proname) AS "name"
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS pn
    ON p.pronamespace = pn.oid
WHERE
    pn.nspname = ANY($1)
    AND p.proname = $2
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_proc'::REGCLASS
            AND d.objid = p.oid
            AND d.deptype = 'e'
    );