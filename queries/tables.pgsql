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
			'data_type': COALESCE(tae."type_name", tc."type_name", t.typname),
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
	JOIN pg_catalog.pg_type t
	    ON a.atttypid = t.oid
	LEFT JOIN LATERAL (
	    SELECT COALESCE(qrn.qualified_name||'[]', te.typname||'[]') AS "type_name"
	    FROM pg_catalog.pg_type AS te
	    LEFT JOIN qualified_rel_name AS qrn
	        ON qrn.rel_id = te.typrelid
	    WHERE te.oid = t.typelem
	) AS tae ON true
	LEFT JOIN LATERAL (
	    SELECT qrn.qualified_name AS "type_name"
	    FROM qualified_rel_name qrn
	    WHERE qrn.rel_id = t.typrelid
	) AS tc ON true
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
), simple_table_columns AS (
   	SELECT a.attrelid, a.attname, a.attnum
   	FROM pg_catalog.pg_attribute a
   	WHERE NOT a.attisdropped
), table_constraints AS (
    SELECT
        t.oid table_oid,
        co.conname AS "name",
        TO_JSONB(CASE co.contype
            WHEN 'c' THEN
                JSON_OBJECT(
                    'type': 'Check',
                    'expression': pg_get_constraintdef(co.oid),
                    'columns': col."columns",
                    'is_inheritable': NOT co.connoinherit
                )
            WHEN 'f' THEN
                JSON_OBJECT(
                    'type': 'ForeignKey',
                    'columns': col."columns",
                    'ref_table': (
                        SELECT JSON_OBJECT(
                            'schema_name': quote_ident(tn.nspname),
                            'local_name': quote_ident(t.relname)
                        )
                        FROM pg_catalog.pg_class AS t
                        JOIN pg_catalog.pg_namespace AS tn ON tn.oid = t.relnamespace
                        WHERE t.oid = co.confrelid
                    ),
                    'ref_columns': (
                        SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
                        FROM simple_table_columns a
                        WHERE
                            a.attrelid = co.conrelid
                            AND a.attnum = ANY(co.confkey)
                    ),
                    'match_type': CASE confmatchtype
                        WHEN 'f' THEN 'Full'
                        WHEN 'p' THEN 'Partial'
                        WHEN 's' THEN 'Simple'
                    END,
                    'on_delete': CASE confdeltype
                        WHEN 'a' THEN JSON_OBJECT('type': 'NoAction')
                        WHEN 'r' THEN JSON_OBJECT('type': 'Restrict')
                        WHEN 'c' THEN JSON_OBJECT('type': 'Cascade')
                        WHEN 'n' THEN JSON_OBJECT(
                            'type': 'SetNull',
                            'columns': (
                                SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
                                FROM simple_table_columns a
                                WHERE
                                    a.attrelid = co.conrelid
                                    AND a.attnum = ANY(co.confdelsetcols)
                            )
                        )
                        WHEN 'd' THEN JSON_OBJECT(
                            'type': 'SetDefault',
                            'columns': (
                                SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
                                FROM simple_table_columns a
                                WHERE
                                    a.attrelid = co.conrelid
                                    AND a.attnum = ANY(co.confdelsetcols)
                            )
                        )
                    END,
                    'on_update': CASE confupdtype
                        WHEN 'a' THEN JSON_OBJECT('type': 'NoAction')
                        WHEN 'r' THEN JSON_OBJECT('type': 'Restrict')
                        WHEN 'c' THEN JSON_OBJECT('type': 'Cascade')
                        WHEN 'n' THEN JSON_OBJECT(
                            'type': 'SetNull',
                            'columns': (
                                SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
                                FROM simple_table_columns a
                                WHERE
                                    a.attrelid = co.conrelid
                                    AND a.attnum = ANY(co.confdelsetcols)
                            )
                        )
                        WHEN 'd' THEN JSON_OBJECT(
                            'type': 'SetDefault',
                            'columns': (
                                SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
                                FROM simple_table_columns a
                                WHERE
                                    a.attrelid = co.conrelid
                                    AND a.attnum = ANY(co.confdelsetcols)
                            )
                        )
                    END
                )
            WHEN 'p' THEN
                JSON_OBJECT(
                    'type': 'PrimaryKey',
                    'columns': col."columns",
                       'index_parameters': JSON_OBJECT(
                           'include': inc."columns",
                           'with': ic.reloptions,
                           'tablespace': its.spcname
                       )
                )
            WHEN 'u' THEN
                JSON_OBJECT(
                    'type': 'Unique',
                    'columns': col."columns",
                    'are_nulls_distinct': (
                        SELECT NOT indnullsnotdistinct
                        FROM pg_catalog.pg_index i
                        WHERE
                            i.indexrelid = co.conindid
                            AND i.indisunique
                    ),
                       'index_parameters': JSON_OBJECT(
                           'include': inc."columns",
                           'with': ic.reloptions,
                           'tablespace': its.spcname
                       )
                )
        END) AS "constraint_type",
        CASE
            WHEN co.condeferrable THEN
                JSON_OBJECT(
                    'type': 'Deferrable',
                    'is_immediate': co.condeferred
                )
            ELSE JSON_OBJECT('type': 'NotDeferrable')
        END AS "timing"
    FROM pg_catalog.pg_constraint co
    JOIN pg_catalog.pg_class t ON t.oid = co.conrelid
    JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
    LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY a.attnum) as "columns"
        FROM simple_table_columns a
        WHERE
            a.attrelid = co.conrelid
            AND a.attnum = ANY(co.conkey)
    ) AS col ON true
    LEFT JOIN pg_catalog.pg_index i
        ON co.conindid = i.indexrelid
    LEFT JOIN pg_catalog.pg_class AS ic
        ON i.indexrelid = ic.oid
    LEFT JOIN pg_catalog.pg_tablespace its
        ON ic.reltablespace = its.oid
    LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY ikey.ord) AS "columns"
        FROM UNNEST(i.indkey) WITH ORDINALITY AS ikey(attnum, ord)
        JOIN pg_catalog.pg_attribute a
            ON a.attrelid = t.oid
            AND a.attnum = ikey.attnum
        WHERE
            ikey.ord > indnkeyatts
    ) AS inc ON true
    WHERE
        co.contype IN ('c','f','p','u')
), table_indexes AS (
    SELECT
        t.oid table_oid,
        JSON_OBJECT(
            'schema_name': quote_ident(tn.nspname),
            'local_name': quote_ident(ic.relname)
        ) AS "name",
        c."columns" AS "columns",
        i.indisvalid AS "is_valid",
        pg_catalog.pg_get_indexdef(ic.oid) AS definition_statement,
        inc."columns" AS "include",
        ic.reloptions AS "with",
        its.spcname AS "tablespace"
    FROM pg_catalog.pg_index AS i
    JOIN pg_catalog.pg_class AS ic
        ON i.indexrelid = ic.oid
    JOIN pg_catalog.pg_class AS t
        ON i.indrelid = t.oid
    JOIN pg_catalog.pg_namespace AS tn
        ON t.relnamespace = tn.oid
    LEFT JOIN pg_catalog.pg_tablespace its
        ON ic.reltablespace = its.oid
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY ikey.ord) AS "columns"
        FROM UNNEST(i.indkey) WITH ORDINALITY AS ikey(attnum, ord)
        JOIN pg_catalog.pg_attribute a
            ON a.attrelid = t.oid
            AND a.attnum = ikey.attnum
        WHERE
            ikey.ord <= indnkeyatts
    ) c
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY ikey.ord) AS "columns"
        FROM UNNEST(i.indkey) WITH ORDINALITY AS ikey(attnum, ord)
        JOIN pg_catalog.pg_attribute a
            ON a.attrelid = t.oid
            AND a.attnum = ikey.attnum
        WHERE
            ikey.ord > indnkeyatts
    ) inc
    WHERE
        NOT i.indisunique
        AND NOT i.indisprimary
        AND NOT i.indisexclusion
), table_triggers AS (
    SELECT
        tc.oid table_oid,
        t.tgname AS "name",
        TO_JSONB(JSON_OBJECT(
            'schema_name': quote_ident(tpn.nspname),
            'local_name': quote_ident(tp.proname)
        )) AS function_name,
		CAST(t.tgtype::INT AS BIT(7)) & b'1000000' >> 6 = b'0000001' AS is_instead,
		CAST(t.tgtype::INT AS BIT(7)) & b'0100000' >> 5 = b'0000001' AS is_truncate,
		CAST(t.tgtype::INT AS BIT(7)) & b'0010000' >> 4 = b'0000001' AS is_update,
		CAST(t.tgtype::INT AS BIT(7)) & b'0001000' >> 3 = b'0000001' AS is_delete,
		CAST(t.tgtype::INT AS BIT(7)) & b'0000100' >> 2 = b'0000001' AS is_insert,
		CAST(t.tgtype::INT AS BIT(7)) & b'0000010' >> 1 = b'0000001' AS is_before,
		CAST(t.tgtype::INT AS BIT(7)) & b'0000001' >> 0 = b'0000001' AS is_row_level,
        pg_get_expr(t.tgqual, tc.oid) AS when_expression,
        t.tgoldtable,
        t.tgnewtable,
        targ.targs,
        ta."columns"
    FROM pg_catalog.pg_trigger AS t
    JOIN pg_catalog.pg_class AS tc
        ON t.tgrelid = tc.oid
    JOIN pg_catalog.pg_namespace AS ton
        ON tc.relnamespace = ton.oid
    JOIN pg_catalog.pg_proc AS tp
        ON t.tgfoid = tp.oid
    JOIN pg_catalog.pg_namespace AS tpn
        ON tp.pronamespace = tpn.oid
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY a.attnum) AS "columns"
        FROM UNNEST(t.tgattr) ta(attnum)
        JOIN simple_table_columns a
            ON a.attrelid = tc.oid
            AND ta.attnum = a.attnum
    ) AS ta
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(GET_BYTE(t.tgargs, targ.i - 1)) targs
        FROM GENERATE_SERIES(1, LENGTH(t.tgargs)) targ(i)
    ) AS targ
    WHERE
        t.tgparentid = 0
        AND NOT t.tgisinternal
)
SELECT
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
	TO_JSONB((
	    SELECT
	        ARRAY_AGG(JSON_OBJECT(
                'name': tc.name,
                'constraint_type': tc.constraint_type,
                'timing': tc.timing
	        ))
	    FROM table_constraints tc
	    WHERE tc.table_oid = t.oid
	)) AS "constraints",
	TO_JSONB((
	    SELECT
	        ARRAY_AGG(JSON_OBJECT(
                'schema_qualified_name': ti.name,
                'columns': ti.columns,
                'is_valid': ti.is_valid,
                'definition_statement': ti.definition_statement,
                'include': ti.include,
                'with': ti.with,
                'tablespace': ti.tablespace
	        ))
	    FROM table_indexes ti
	    WHERE ti.table_oid = t.oid
	)) AS "indexes",
	TO_JSONB((
	    SELECT
	        ARRAY_AGG(JSON_OBJECT(
                'name': tt.name,
                'timing': CASE
                    WHEN tt.is_before THEN 'before'
                    WHEN tt.is_instead THEN 'instead-of'
                    ELSE 'after'
                END,
                'events':
                CASE
                    WHEN tt.is_insert THEN ARRAY[JSON_OBJECT('type': 'insert')]
                    ELSE '{}'
                END ||
                CASE
                    WHEN tt.is_update THEN ARRAY[JSON_OBJECT('type': 'update', 'columns': tt."columns")]
                    ELSE '{}'
                END ||
                CASE
                    WHEN tt.is_delete THEN ARRAY[JSON_OBJECT('type': 'delete')]
                    ELSE '{}'
                END ||
                CASE
                    WHEN tt.is_truncate THEN ARRAY[JSON_OBJECT('type': 'truncate')]
                    ELSE '{}'
                END,
                'old_name': tt.tgoldtable,
                'new_name': tgnewtable,
                'is_row_level': tt.is_row_level,
                'when_expression': tt.when_expression,
                'function_name': tt.function_name,
                'function_args': tt.targs
	        ))
	    FROM table_triggers tt
	    WHERE tt.table_oid = t.oid
	)) AS "triggers",
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