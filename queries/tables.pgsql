WITH table_columns AS (
    SELECT
	    a.attrelid,
		ARRAY_AGG(JSON_OBJECT(
			'name': a.attname,
			'data_type': pg_catalog.format_type(a.atttypid, a.atttypmod),
			'size': a.attlen,
			'collation': '"'||cn.nspname||'"."'||cl.collname||'"',
			'is_non_null': attnotnull,
			'default_expression': CASE
			    WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(def.adbin, def.adrelid)
			END,
			'generated_column': CASE
			    WHEN a.attgenerated = 's' THEN
			        JSON_OBJECT(
			            'expression': pg_catalog.pg_get_expr(def.adbin, def.adrelid),
			            'generation_type': 'Stored'
			        )
			END,
			'identity_column': CASE
                WHEN a.attidentity IN ('a','d') THEN
                    JSON_OBJECT(
                        'identity_generation': CASE
                            WHEN a.attidentity = 'a' THEN 'Always'
                            WHEN a.attidentity = 'd' THEN 'Default'
                        END,
                        'sequence_options': (
                            SELECT JSON_OBJECT(
                                'increment': s.seqincrement,
                                'min_value': s.seqmin,
                                'max_value': s.seqmax,
                                'start_value': s.seqstart,
                                'cache': s.seqcache,
                                'is_cycle': s.seqcycle
                            )
                            FROM pg_catalog.pg_sequence s
                            JOIN pg_catalog.pg_depend AS sd
                                 ON sd.classid = 'pg_class'::REGCLASS
                                 AND s.seqrelid = sd.objid
                                 AND sd.refclassid = 'pg_class'::REGCLASS
                                 AND sd.deptype = 'i'
                            WHERE
                                sd.refobjid = a.attrelid
                                AND sd.refobjsubid = a.attnum
                        )
                    )
            END,
            'storage': a.attstorage,
            'compression': a.attcompression
		) ORDER BY a.attnum) AS "columns"
	FROM pg_catalog.pg_attribute AS a
	LEFT JOIN pg_catalog.pg_collation AS cl
	    ON a.attcollation = cl.oid
	LEFT JOIN pg_catalog.pg_namespace AS cn
	    ON cl.collnamespace = cn.oid
	LEFT JOIN pg_catalog.pg_attrdef AS def
	    ON a.attrelid = def.adrelid
	    AND a.attnum = def.adnum
	WHERE
	    a.attnum > 0
	    AND NOT a.attisdropped
	GROUP BY a.attrelid
)
SELECT
    t.oid,
	TO_JSONB(JSON_OBJECT(
		'schema_name': quote_ident(tn.nspname),
		'local_name': quote_ident(t.relname)
	)) AS "name",
	TO_JSONB(c."columns") AS "columns",
	CASE
        WHEN t.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(t.oid)
        ELSE NULL
    END AS "partition_key_def",
	CASE
        WHEN t.relispartition THEN pg_catalog.pg_get_expr(t.relpartbound, t.oid)
        ELSE NULL
    END AS "partition_values",
	TO_JSONB(pi.inherited_tables) AS "inherited_tables",
	pp.partitioned_parent_table,
    tts.spcname AS "tablespace",
    t.reloptions AS "with",
    TO_JSONB('{}'::json[]) AS "dependencies"
FROM pg_catalog.pg_class AS t
JOIN pg_catalog.pg_namespace AS tn
	ON t.relnamespace = tn.oid
LEFT JOIN pg_catalog.pg_tablespace tts
	ON t.reltablespace = tts.oid
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'schema_name': quote_ident(pn.nspname),
            'local_name': quote_ident(pt.relname)
        )) AS "inherited_tables"
    FROM pg_catalog.pg_inherits AS p
    LEFT JOIN pg_catalog.pg_class AS pt
        ON p.inhparent = pt.oid
    LEFT JOIN pg_catalog.pg_namespace AS pn
        ON pt.relnamespace = pn.oid
    WHERE
        pt.relkind != 'p'
        AND t.oid = p.inhrelid
) AS pi
LEFT JOIN LATERAL (
    SELECT
        JSON_OBJECT(
            'schema_name': quote_ident(pn.nspname),
            'local_name': quote_ident(pt.relname)
        ) AS "partitioned_parent_table"
    FROM pg_catalog.pg_inherits AS p
    LEFT JOIN pg_catalog.pg_class AS pt
        ON p.inhparent = pt.oid
    LEFT JOIN pg_catalog.pg_namespace AS pn
        ON pt.relnamespace = pn.oid
    WHERE
        pt.relkind = 'p'
        AND t.oid = p.inhrelid
) AS pp ON true
JOIN table_columns AS c
    ON c.attrelid = t.oid
WHERE
    tn.nspname = ANY($1)
	AND t.relkind IN ('r','p')
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );