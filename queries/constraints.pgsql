WITH table_columns AS (
	SELECT a.attrelid, a.attname, a.attnum
	FROM pg_catalog.pg_attribute a
	WHERE NOT a.attisdropped
)
SELECT
	co.conname AS "name",
	TO_JSONB(JSON_OBJECT(
		'schema_name': tn.nspname,
		'local_name': t.relname
	)) AS "owning_table",
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
						'schema_name': tn.nspname,
						'local_name': t.relname
					)
					FROM pg_catalog.pg_class AS t
					JOIN pg_catalog.pg_namespace AS tn ON tn.oid = t.relnamespace
					WHERE t.oid = co.confrelid
				),
				'ref_columns': (
					SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
					FROM table_columns a
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
							FROM table_columns a
							WHERE
								a.attrelid = co.conrelid
								AND a.attnum = ANY(co.confdelsetcols)
						)
					)
					WHEN 'd' THEN JSON_OBJECT(
						'type': 'SetDefault',
						'columns': (
							SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
							FROM table_columns a
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
							FROM table_columns a
							WHERE
								a.attrelid = co.conrelid
								AND a.attnum = ANY(co.confdelsetcols)
						)
					)
					WHEN 'd' THEN JSON_OBJECT(
						'type': 'SetDefault',
						'columns': (
							SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
							FROM table_columns a
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
	FROM table_columns a
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
    tn.nspname = ANY($1)
    AND co.contype IN ('c','f','p','u')
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );