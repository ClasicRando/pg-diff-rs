WITH simple_table_columns AS (
    SELECT a.attrelid, a.attname, a.attnum
    FROM pg_catalog.pg_attribute a
    WHERE NOT a.attisdropped
), table_triggers AS (
    SELECT
        t.oid,
        tc.oid owner_oid,
        t.tgname AS "name",
        TO_JSONB(JSON_OBJECT(
            'schema_name': quote_ident(ton.nspname),
            'local_name': quote_ident(tc.relname)
        )) AS owner_object_name,
        TO_JSONB(JSON_OBJECT(
            'schema_name': quote_ident(ton.nspname),
            'local_name': quote_ident(tc.relname)||'.'||quote_ident(t.tgname)
        )) AS schema_qualified_name,
        TO_JSONB(JSON_OBJECT(
            'schema_name': quote_ident(tpn.nspname),
            'local_name': quote_ident(tp.proname)
        )) AS function_name,
        tp.oid function_oid,
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
        t.tgargs,
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
    WHERE
        t.tgparentid = 0
        AND NOT t.tgisinternal
)
SELECT
    tt.oid,
    tt.owner_oid,
    tt.name,
    tt.schema_qualified_name,
    tt.owner_object_name,
    CASE
        WHEN tt.is_before THEN 'before'
        WHEN tt.is_instead THEN 'instead-of'
        ELSE 'after'
    END AS timing,
    TO_JSONB(
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
        END
    ) AS events,
    tt.tgoldtable AS old_name,
    tt.tgnewtable AS new_name,
    tt.is_row_level AS is_row_level,
    tt.when_expression AS when_expression,
    tt.function_name AS function_name,
    tt.tgargs AS function_args,
    TO_JSONB(ARRAY[owner_object_name, function_name]) AS "dependencies"
FROM table_triggers tt
WHERE
    tt.owner_oid = ANY($1)
    -- Exclude triggers owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = tt.owner_oid
            AND d.deptype = 'e'
    );