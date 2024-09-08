WITH table_indexes AS (
    SELECT
        i.indexrelid AS "oid",
        t.oid table_oid,
        JSON_OBJECT(
            'schema_name': quote_ident(tn.nspname),
            'local_name': quote_ident(t.relname)
        ) AS "owner_table_name",
        JSON_OBJECT(
            'schema_name': quote_ident(tn.nspname),
            'local_name': quote_ident(ic.relname)
        ) AS "name",
        c."columns" AS "columns",
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
    LEFT JOIN pg_catalog.pg_tablespace AS its
        ON ic.reltablespace = its.oid
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY ikey.ord) AS "columns"
        FROM UNNEST(i.indkey) WITH ORDINALITY AS ikey(attnum, ord)
        JOIN pg_catalog.pg_attribute AS a
            ON a.attrelid = t.oid
            AND a.attnum = ikey.attnum
        WHERE
            ikey.ord <= indnkeyatts
    ) c
    CROSS JOIN LATERAL (
        SELECT ARRAY_AGG(a.attname ORDER BY ikey.ord) AS "columns"
        FROM UNNEST(i.indkey) WITH ORDINALITY AS ikey(attnum, ord)
        JOIN pg_catalog.pg_attribute AS a
            ON a.attrelid = t.oid
            AND a.attnum = ikey.attnum
        WHERE
            ikey.ord > indnkeyatts
    ) inc
    WHERE
        NOT i.indisunique
        AND NOT i.indisprimary
        AND NOT i.indisexclusion
)
SELECT
    ti.oid,
    ti.table_oid,
    ti.owner_table_name,
    ti.name AS schema_qualified_name,
    ti.columns,
    ti.definition_statement,
    ti.include,
    ti.with,
    ti.tablespace,
    TO_JSONB(ARRAY[ti.owner_table_name]) AS "dependencies"
FROM table_indexes AS ti
WHERE
    ti.table_oid = ANY($1)
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = ti.table_oid
            AND d.deptype = 'e'
    );