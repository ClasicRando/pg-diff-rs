WITH simple_table_columns AS (
    SELECT a.attrelid, a.attname, a.attnum
    FROM pg_catalog.pg_attribute AS a
    WHERE NOT a.attisdropped
), table_constraints AS (
    SELECT
		co.oid oid,
        t.oid table_oid,
        co.conname AS "name",
        JSON_OBJECT(
            'schema_name': quote_ident(tn.nspname),
            'local_name': quote_ident(t.relname)
        ) AS "owner_table_name",
        JSON_OBJECT(
            'schema_name': quote_ident(tn.nspname),
            'local_name': quote_ident(co.conname)
        ) AS "schema_qualified_name",
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
                        JOIN pg_catalog.pg_namespace AS tn
                            ON tn.oid = t.relnamespace
                        WHERE t.oid = co.confrelid
                    ),
                    'ref_columns': (
                        SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
                        FROM simple_table_columns AS a
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
                                FROM simple_table_columns AS a
                                WHERE
                                    a.attrelid = co.conrelid
                                    AND a.attnum = ANY(co.confdelsetcols)
                            )
                        )
                        WHEN 'd' THEN JSON_OBJECT(
                            'type': 'SetDefault',
                            'columns': (
                                SELECT ARRAY_AGG(a.attname ORDER BY a.attnum)
                                FROM simple_table_columns AS a
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
                        FROM pg_catalog.pg_index AS i
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
    FROM pg_catalog.pg_constraint AS co
    JOIN pg_catalog.pg_class AS t
        ON t.oid = co.conrelid
    JOIN pg_catalog.pg_namespace AS tn
        ON tn.oid = t.relnamespace
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY a.attnum) as "columns"
        FROM simple_table_columns a
        WHERE
            a.attrelid = co.conrelid
            AND a.attnum = ANY(co.conkey)
    ) AS col
    LEFT JOIN pg_catalog.pg_index AS i
        ON co.conindid = i.indexrelid
    LEFT JOIN pg_catalog.pg_class AS ic
        ON i.indexrelid = ic.oid
    LEFT JOIN pg_catalog.pg_tablespace AS its
        ON ic.reltablespace = its.oid
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY ikey.ord) AS "columns"
        FROM UNNEST(i.indkey) WITH ORDINALITY AS ikey(attnum, ord)
        JOIN pg_catalog.pg_attribute AS a
            ON a.attrelid = t.oid
            AND a.attnum = ikey.attnum
        WHERE
            ikey.ord > indnkeyatts
    ) AS inc
    WHERE
        co.contype IN ('c','f','p','u')
)
SELECT
	tc.oid,
    tc.table_oid,
    tc.owner_table_name,
    tc.name,
    tc.schema_qualified_name,
    tc.constraint_type,
    tc.timing,
	TO_JSONB(td.dependencies) AS "dependencies"
FROM table_constraints AS tc
CROSS JOIN LATERAL (
	SELECT
	    ARRAY_AGG(JSON_OBJECT(
            'catalog': 'pg_class',
            'oid': CAST(td.oid AS integer)
        )) AS "dependencies"
	FROM (
		SELECT DISTINCT td.oid
		FROM pg_catalog.pg_depend AS d
		JOIN pg_catalog.pg_class AS td
			ON d.refclassid = 'pg_class'::REGCLASS
			AND d.refobjid = td.oid
		WHERE
			d.classid = 'pg_constraint'::REGCLASS
			AND d.objid = tc.oid
			AND d.deptype IN ('n','a')
			AND td.relkind IN ('r','p')
	) AS td
) AS td
WHERE
    tc.table_oid = ANY($1)
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = tc.table_oid
            AND d.deptype = 'e'
    );