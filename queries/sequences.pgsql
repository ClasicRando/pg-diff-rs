SELECT
    s.seqrelid AS "oid",
	TO_JSONB(JSON_OBJECT(
		'schema_name': quote_ident(sn.nspname),
		'local_name': quote_ident(sc.relname)
	)) AS "name",
	pg_catalog.format_type(s.seqtypid, null) AS "data_type",
	s.seqincrement AS increment,
	s.seqmin AS min_value,
	s.seqmax AS max_value,
	s.seqstart AS start_value,
	s.seqcache AS "cache",
	s.seqcycle AS "is_cycle",
	CASE
	    WHEN sa.attnum IS NOT NULL THEN
	        TO_JSONB(JSON_OBJECT(
	            'table_name': JSON_OBJECT(
                    'schema_name': quote_ident(son.nspname),
                    'local_name': quote_ident(so.relname)
	            ),
	            'name': sa.attname
	        ))
	END AS "owner",
	TO_JSONB(
        CASE
            WHEN sa.attnum IS NOT NULL THEN
                ARRAY[JSON_OBJECT(
                    'catalog': 'pg_class',
                    'oid': CAST(so.oid AS integer)
                )]
            ELSE
                ARRAY[JSON_OBJECT(
                    'catalog': 'pg_namespace',
                    'oid': CAST(sn.oid AS integer)
                )]
        END
    ) AS "dependencies"
FROM pg_catalog.pg_sequence AS s
JOIN pg_catalog.pg_class AS sc
    ON s.seqrelid = sc.oid
JOIN pg_catalog.pg_namespace AS sn
	ON sc.relnamespace = sn.oid
JOIN pg_catalog.pg_type AS st
    ON s.seqtypid = st.oid
LEFT JOIN pg_catalog.pg_depend AS sd
    ON sd.classid = 'pg_class'::REGCLASS
    AND s.seqrelid = sd.objid
    AND sd.refclassid = 'pg_class'::REGCLASS
    AND sd.deptype = 'a'
LEFT JOIN pg_catalog.pg_attribute AS sa
    ON sd.refobjid = sa.attrelid
    AND sd.refobjsubid = sa.attnum
LEFT JOIN pg_catalog.pg_class AS so
    ON sd.refobjid = so.oid
LEFT JOIN pg_catalog.pg_namespace AS son
    ON so.relnamespace = son.oid
WHERE
    sn.nspname = ANY($1)
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND s.seqrelid = d.objid
            AND d.refclassid = 'pg_class'::REGCLASS
            AND d.deptype = 'i'
    )
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = sc.oid
            AND d.deptype = 'e'
    );
