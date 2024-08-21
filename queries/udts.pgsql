SELECT
    TO_JSONB(JSON_OBJECT(
        'schema_name': quote_ident(tn.nspname),
        'local_name': quote_ident(t.typname)
    )) AS "name",
    TO_JSONB(CASE t.typtype
        WHEN 'c' THEN JSON_OBJECT(
            'type': 'Composite',
            'attributes': (
                SELECT
                    ARRAY_AGG(JSON_OBJECT(
                        'name': a.attname,
                        'data_type': at.typname,
                        'size': a.attlen,
                        'collation': '"'||cn.nspname||'"."'||cl.collname||'"'
                    ) ORDER BY a.attnum) AS "columns"
                FROM pg_catalog.pg_attribute AS a
                JOIN pg_catalog.pg_type AS at
                    ON a.atttypid = at.oid
                LEFT JOIN pg_catalog.pg_collation cl
                    ON a.attcollation = cl.oid
                LEFT JOIN pg_catalog.pg_namespace cn
                    ON cl.collnamespace = cn.oid
                WHERE
                    a.attnum > 0
                    AND NOT a.attisdropped
                    AND a.attrelid = t.typrelid
            )
        )
        WHEN 'e' THEN JSON_OBJECT(
            'type': 'Enum',
            'labels': (
                SELECT ARRAY_AGG(e.enumlabel ORDER BY e.enumsortorder)
                FROM pg_catalog.pg_enum AS e
                WHERE t.oid = e.enumtypid
            )
        )
        WHEN 'r' THEN JSON_OBJECT(
            'type': 'Range',
            'subtype': (
                SELECT trs.typname
                FROM pg_catalog.pg_range tr
                JOIN pg_catalog.pg_type trs
                    ON tr.rngsubtype = trs.oid
                WHERE t.oid = tr.rngtypid
            )
        )
    END) AS "udt_type"
FROM pg_catalog.pg_type AS t
JOIN pg_catalog.pg_namespace AS tn
	ON t.typnamespace = tn.oid
WHERE
    (
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
    )
    AND tn.nspname = ANY($1)
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_type'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );
