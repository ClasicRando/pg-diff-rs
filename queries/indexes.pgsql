SELECT
    ic.relname AS "name",
    TO_JSONB(JSON_OBJECT(
        'schema_name': tn.nspname,
        'local_name': t.relname
    )) AS "owning_table",
    c."columns" AS "columns",
    i.indisvalid AS "is_valid",
    pg_catalog.pg_get_indexdef(ic.oid) AS definition_statement,
    inc."columns" AS "include",
    (
        SELECT
            JSON_OBJECT_AGG(
                SUBSTRING(opt FROM 1 FOR POSITION('=' IN opt) - 1),
                SUBSTRING(opt FROM POSITION('=' IN opt) + 1)
            )
        FROM UNNEST(ic.reloptions) iopt(opt)
    ) AS "with",
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
    tn.nspname = ANY($1)
    AND NOT i.indisunique
    AND NOT i.indisprimary
    AND NOT i.indisexclusion
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );
