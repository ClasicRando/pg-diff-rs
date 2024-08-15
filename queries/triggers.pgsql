SELECT
    t.tgname AS "name",
	TO_JSONB(JSON_OBJECT(
		'schema_name': ton.nspname,
		'local_name': tc.relname
	)) AS "owning_table",
	TO_JSONB(JSON_OBJECT(
		'schema_name': tpn.nspname,
		'local_name': tp.proname
	)) AS "function",
    pg_catalog.pg_get_function_identity_arguments(tp.oid) AS "function_signature",
    pg_catalog.pg_get_triggerdef(t.oid) AS "definition"
FROM pg_catalog.pg_trigger AS t
JOIN pg_catalog.pg_class AS tc
    ON t.tgrelid = tc.oid
JOIN pg_catalog.pg_namespace AS ton
    ON tc.relnamespace = ton.oid
JOIN pg_catalog.pg_proc AS tp
    ON t.tgfoid = tp.oid
JOIN pg_catalog.pg_namespace AS tpn
    ON tp.pronamespace = tpn.oid
WHERE
    ton.nspname = ANY($1)
    AND t.tgparentid = 0
    AND NOT t.tgisinternal
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = tc.oid
            AND d.deptype = 'e'
    );