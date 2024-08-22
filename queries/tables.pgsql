WITH roles AS (
    SELECT r.oid, r.rolname
    FROM pg_catalog.pg_roles r
    UNION
    SELECT 0 AS "oid", 'PUBLIC' AS role_name
), qualified_rel_name AS (
    SELECT c.oid AS rel_id, '"'||cn.nspname||'"."'||c.relname||'"' AS qualified_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace cn
        ON c.relnamespace = cn.oid
), table_columns AS (
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
	FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_collation cl
	    ON a.attcollation = cl.oid
	LEFT JOIN pg_catalog.pg_namespace cn
	    ON cl.collnamespace = cn.oid
	LEFT JOIN pg_catalog.pg_attrdef def
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
	TO_JSONB((
		SELECT
			ARRAY_AGG(JSON_OBJECT(
				'name': pol.polname,
				'schema_qualified_name': JSON_OBJECT(
                    'schema_name': quote_ident(tn.nspname),
                    'local_name': quote_ident(pol.polname)
                ),
				'owner_table': JSON_OBJECT(
                    'schema_name': quote_ident(tn.nspname),
                    'local_name': quote_ident(t.relname)
                ),
				'is_permissive': pol.polpermissive,
				'applies_to': (
					SELECT ARRAY_AGG(rolname)
					FROM roles
					WHERE roles.oid = ANY(pol.polroles)
				),
				'command': pol.polcmd,
				'check_expression': pg_catalog.pg_get_expr(
					pol.polwithcheck,
					pol.polrelid
				),
				'using_expression': pg_catalog.pg_get_expr(
					pol.polqual,
					pol.polrelid
				),
				'columns': (
					SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
					FROM pg_catalog.pg_attribute AS a
					INNER JOIN pg_catalog.pg_depend AS d ON a.attnum = d.refobjsubid
					WHERE
						d.objid = pol.oid
						AND d.refobjid = t.oid
						AND d.refclassid = 'pg_class'::REGCLASS
						AND a.attrelid = t.oid
						AND NOT a.attisdropped
						AND a.attnum > 0
				)
			))
		FROM pg_catalog.pg_policy as pol
		WHERE t.oid = pol.polrelid
	)) AS "policies",
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
    t.reloptions AS "with"
FROM pg_catalog.pg_class AS t
JOIN pg_catalog.pg_namespace AS tn
	ON t.relnamespace = tn.oid
LEFT JOIN pg_catalog.pg_tablespace tts
	ON t.reltablespace = tts.oid
LEFT JOIN LATERAL (
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
) AS pi ON true
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
JOIN table_columns AS c ON c.attrelid = t.oid
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