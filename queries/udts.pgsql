WITH custom_types AS (
    SELECT ct.oid, ct.typtype, ct.typname, ct.typrelid, ct.typnamespace
    FROM pg_catalog.pg_type AS ct
    WHERE
        ct.typtype IN ('e','r')
        OR
        (
            ct.typtype = 'c'
            AND EXISTS(
                SELECT NULL
                FROM pg_catalog.pg_class tc
                WHERE
                    tc.oid = ct.typrelid
                    AND tc.relkind = 'c'
            )
        )
)
SELECT
    t.oid,
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
                        'data_type': pg_catalog.format_type(a.atttypid, a.atttypmod),
                        'size': a.attlen,
                        'collation': '"'||cn.nspname||'"."'||cl.collname||'"',
                        'is_base_type': t.typtype = 'b'
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
    END) AS "udt_type",
    TO_JSONB(COALESCE(td.dependencies || tyd.dependencies, '{}')) AS "dependencies"
FROM custom_types AS t
JOIN pg_catalog.pg_namespace AS tn
	ON t.typnamespace = tn.oid
CROSS JOIN LATERAL (
	SELECT
	    ARRAY_AGG(JSON_OBJECT(
            'catalog': 'pg_class',
            'oid': CAST(td.oid AS integer)
        )) AS "dependencies"
	FROM (
		SELECT DISTINCT td.oid
		FROM pg_catalog.pg_depend d
		JOIN pg_catalog.pg_class td
			ON d.refclassid = 'pg_class'::REGCLASS
			AND d.refobjid = td.oid
		WHERE
			d.classid = 'pg_type'::REGCLASS
			AND d.objid = t.oid
			AND d.deptype IN ('n','a')
			AND td.relkind IN ('r','p')
	) td
) td
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'catalog': 'pg_type',
            'oid': CAST(tyd.oid AS integer)
        )) AS "dependencies"
    FROM (
        SELECT DISTINCT tyd.oid
        FROM pg_catalog.pg_depend AS d
        JOIN custom_types AS tyd
            ON d.refclassid = 'pg_type'::REGCLASS
            AND d.refobjid = tyd.oid
        WHERE
            d.classid = 'pg_type'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'n'
    ) tyd
) tyd
WHERE
    tn.nspname = ANY($1)
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_type'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );
