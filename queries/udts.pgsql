WITH custom_types AS (
    SELECT
        ct.oid,
        ct.typtype,
        ct.typname,
        ct.typrelid,
        ctn.nspname,
        ct.typnotnull,
        ctc.collname,
        ct.typdefault,
        ct.typbasetype,
        ct.typtypmod,
		ARRAY[JSON_OBJECT(
            'schema_name': quote_ident(ctn.nspname),
            'local_name': ''
        )] AS "dependencies"
    FROM pg_catalog.pg_type AS ct
    JOIN pg_catalog.pg_namespace AS ctn
        ON ct.typnamespace = ctn.oid
    LEFT JOIN pg_catalog.pg_collation AS ctc
        ON ct.typcollation = ctc.oid
    WHERE
        ct.typtype IN ('e','r','d')
        OR
        (
            ct.typtype = 'c'
            AND EXISTS(
                SELECT NULL
                FROM pg_catalog.pg_class AS tc
                WHERE
                    tc.oid = ct.typrelid
                    AND tc.relkind = 'c'
            )
        )
)
SELECT
    t.oid,
    TO_JSONB(JSON_OBJECT(
        'schema_name': quote_ident(t.nspname),
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
                LEFT JOIN pg_catalog.pg_collation AS cl
                    ON a.attcollation = cl.oid
                LEFT JOIN pg_catalog.pg_namespace AS cn
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
                FROM pg_catalog.pg_range AS tr
                JOIN pg_catalog.pg_type AS trs
                    ON tr.rngsubtype = trs.oid
                WHERE
                    t.oid = tr.rngtypid
            )
        )
        WHEN 'd' THEN JSON_OBJECT(
            'type': 'Domain',
            'data_type': pg_catalog.format_type(t.typbasetype, t.typtypmod),
            'collation': t.collname,
            'default': t.typdefault,
            'is_not_null': t.typnotnull,
            'checks': (
                SELECT
                    ARRAY_AGG(JSON_OBJECT(
                        'name': dc.conname,
                        'expression': pg_get_constraintdef(dc.oid)
                    ) ORDER BY dc.conname)
                FROM pg_catalog.pg_constraint dc
                WHERE
                    dc.contypid = t.oid
            )
        )
        WHEN 'b' THEN JSON_OBJECT('type': 'Base')
        WHEN 'p' THEN JSON_OBJECT(
            'type': 'Pseudo'
        )
        WHEN 'm' THEN JSON_OBJECT(
            'type': 'Multirange'
        )
    END) AS "udt_type",
    TO_JSONB(t.dependencies || td.dependencies || tyd.dependencies) AS "dependencies"
FROM custom_types AS t
CROSS JOIN LATERAL (
	SELECT
	    ARRAY_AGG(JSON_OBJECT(
            'schema_name': quote_ident(td.nspname),
            'local_name': quote_ident(td.relname)
        )) AS "dependencies"
	FROM (
		SELECT DISTINCT td.relname, tdn.nspname
		FROM pg_catalog.pg_depend AS d
		JOIN pg_catalog.pg_class AS td
			ON d.refclassid = 'pg_class'::REGCLASS
			AND d.refobjid = td.oid
		JOIN pg_catalog.pg_namespace AS tdn
			ON td.relnamespace = tdn.oid
		WHERE
			d.classid = 'pg_type'::REGCLASS
			AND d.objid = t.oid
			AND d.deptype = 'n'
			AND td.relkind IN ('r','p')
	) td
) td
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'schema_name': quote_ident(tyd.nspname),
            'local_name': quote_ident(tyd.typname)
        )) AS "dependencies"
    FROM (
        SELECT DISTINCT tyd.typname, tyd.nspname
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
    t.nspname = ANY($1)
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_type'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );
