WITH table_columns AS (
	SELECT a.attrelid, a.attname, a.attnum
	FROM pg_catalog.pg_attribute a
	WHERE NOT a.attisdropped
)
SELECT
	co.conname AS "name",
	to_jsonb(json_object(
		'schema_name': tn.nspname,
		'local_name': t.relname
	)) AS "owning_table",
	to_jsonb(CASE co.contype
		WHEN 'c' THEN
			json_object(
				'type': 'Check',
				'expression': pg_get_constraintdef(co.oid),
				'columns': col."columns",
				'is_inheritable': NOT co.connoinherit
			)
		WHEN 'p' THEN
			json_object(
				'type': 'PrimaryKey',
				'columns': col."columns"
			)
		WHEN 'f' THEN
			json_object(
				'type': 'ForeignKey',
				'columns': col."columns",
				'ref_table': (
					SELECT json_object(
						'schema_name': tn.nspname,
						'local_name': t.relname
					)
					FROM pg_catalog.pg_class AS t
					JOIN pg_catalog.pg_namespace AS tn ON tn.oid = t.relnamespace
					WHERE t.oid = co.confrelid
				),
				'ref_columns': (
					SELECT ARRAY_AGG(a.attname)
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
					WHEN 'a' THEN json_object('type': 'NoAction')
					WHEN 'r' THEN json_object('type': 'Restrict')
					WHEN 'c' THEN json_object('type': 'Cascade')
					WHEN 'n' THEN json_object(
						'type': 'SetNull',
						'columns': (
							SELECT ARRAY_AGG(a.attname)
							FROM table_columns a
							WHERE
								a.attrelid = co.conrelid
								AND a.attnum = ANY(co.confdelsetcols)
						)
					)
					WHEN 'd' THEN json_object(
						'type': 'SetDefault',
						'columns': (
							SELECT ARRAY_AGG(a.attname)
							FROM table_columns a
							WHERE
								a.attrelid = co.conrelid
								AND a.attnum = ANY(co.confdelsetcols)
						)
					)
				END,
				'on_update': CASE confupdtype
					WHEN 'a' THEN json_object('type': 'NoAction')
					WHEN 'r' THEN json_object('type': 'Restrict')
					WHEN 'c' THEN json_object('type': 'Cascade')
					WHEN 'n' THEN json_object(
						'type': 'SetNull',
						'columns': (
							SELECT ARRAY_AGG(a.attname)
							FROM table_columns a
							WHERE
								a.attrelid = co.conrelid
								AND a.attnum = ANY(co.confdelsetcols)
						)
					)
					WHEN 'd' THEN json_object(
						'type': 'SetDefault',
						'columns': (
							SELECT ARRAY_AGG(a.attname)
							FROM table_columns a
							WHERE
								a.attrelid = co.conrelid
								AND a.attnum = ANY(co.confdelsetcols)
						)
					)
				END
			)
		WHEN 'u' THEN
			json_object(
				'type': 'Unique',
				'columns': col."columns",
				'are_nulls_distinct': (
					SELECT NOT indnullsnotdistinct
					FROM pg_catalog.pg_index i
					WHERE
						i.indexrelid = co.conindid
						AND i.indisunique
				)
			)
	END) AS "constraint_type",
	CASE
		WHEN co.condeferrable THEN
			json_object(
				'type': 'Deferrable',
				'is_immediate': co.condeferred
			)
		ELSE json_object('type': 'NotDeferrable')
	END AS "timing",
	co.conkey
FROM pg_catalog.pg_constraint co
JOIN pg_catalog.pg_class t ON t.oid = co.conrelid
JOIN pg_catalog.pg_namespace tn ON tn.oid = t.relnamespace
LEFT JOIN LATERAL (
	SELECT ARRAY_AGG(a.attname) as "columns"
	FROM table_columns a
	WHERE
		a.attrelid = co.conrelid
		AND a.attnum = ANY(co.conkey)
) AS col ON true
WHERE
    tn.nspname = $1
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT d.objid
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );