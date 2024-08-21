SELECT
	TO_JSONB(JSON_OBJECT(
		'schema_name': quote_ident(v.schemaname),
		'local_name': quote_ident(v.viewname)
	)) AS "name",
	(
		SELECT
			ARRAY_AGG(a.attname ORDER BY a.attnum) AS "columns"
		FROM pg_catalog.pg_attribute a
		WHERE
			a.attnum > 0
			AND NOT a.attisdropped
			AND a.attrelid = vc.oid
	) AS "columns",
	v.definition AS "query",
	vc.reloptions AS "options"
FROM pg_catalog.pg_views v
JOIN pg_catalog.pg_namespace vn
	ON v.schemaname = vn.nspname
JOIN pg_catalog.pg_class vc
    ON v.viewname = vc.relname
	AND vc.relnamespace = vn.oid
WHERE
    v.schemaname = ANY($1)
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = vc.oid
            AND d.deptype = 'e'
    );
