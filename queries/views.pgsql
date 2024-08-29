WITH custom_types AS (
    SELECT ct.oid, ct.typtype, ct.typname, ct.typrelid, ct.typnamespace
    FROM pg_catalog.pg_type AS ct
    WHERE
        ct.typtype IN ('e','r')
        OR
        (
            ct.typtype = 'c'
            AND EXISTS(
                SELECT NULL
                FROM pg_catalog.pg_class AS tc
                WHERE
                    tc.oid = ct.typrelid
                    AND tc.relkind = 'c'
            )
        )
), query_views AS (
	SELECT
		vc.oid,
		TO_JSONB(JSON_OBJECT(
			'schema_name': quote_ident(vn.nspname),
			'local_name': quote_ident(vc.relname)
		)) AS "name",
		vn.nspname,
		(
			SELECT
				ARRAY_AGG(a.attname ORDER BY a.attnum) AS "columns"
			FROM pg_catalog.pg_attribute AS a
			WHERE
				a.attnum > 0
				AND NOT a.attisdropped
				AND a.attrelid = vc.oid
		) AS "columns",
		pg_get_viewdef(vc.oid) AS "query",
		vc.reloptions AS "options",
		ARRAY[JSON_OBJECT(
            'oid': CAST(vn.oid AS INTEGER),
            'catalog': 'pg_namespace'
        )] AS "dependencies"
	FROM pg_catalog.pg_class AS vc
	JOIN pg_catalog.pg_namespace AS vn
		ON vc.relnamespace = vn.oid
	WHERE
		vc.relkind = 'v'
		-- Exclude tables owned by extensions
		AND NOT EXISTS (
			SELECT NULL
			FROM pg_catalog.pg_depend AS d
			WHERE
				d.classid = 'pg_class'::REGCLASS
				AND d.objid = vc.oid
				AND d.deptype = 'e'
		)
)
SELECT
	v.oid, v.name, v.columns, v.query, v.options,
	TO_JSONB(v.dependencies || cd.dependencies || tyd.dependencies) AS "dependencies"
FROM pg_catalog.pg_rewrite AS r
JOIN query_views AS v
	ON r.ev_class = v.oid
CROSS JOIN LATERAL (
	SELECT
	    ARRAY_AGG(JSON_OBJECT(
            'oid': CAST(cd.oid AS integer),
            'catalog': 'pg_class'
        )) AS "dependencies"
	FROM (
		SELECT DISTINCT cd.oid
		FROM pg_catalog.pg_depend AS d
		JOIN pg_catalog.pg_class AS cd
			ON d.refclassid = 'pg_class'::REGCLASS
			AND d.refobjid = cd.oid
		WHERE
			d.classid = 'pg_rewrite'::REGCLASS
			AND d.objid = r.oid
			AND d.deptype = 'n'
			AND cd.relkind IN ('r','p','v')
	) AS cd
) AS cd
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'oid': CAST(tyd.oid AS integer),
            'catalog': 'pg_type'
        )) AS "dependencies"
    FROM (
        SELECT DISTINCT tyd.oid
        FROM pg_catalog.pg_depend AS d
        JOIN custom_types AS tyd
            ON d.refclassid = 'pg_type'::REGCLASS
            AND d.refobjid = tyd.oid
        WHERE
            d.classid = 'pg_rewrite'::REGCLASS
            AND d.objid = r.oid
            AND d.deptype = 'n'
    ) AS tyd
) AS tyd
WHERE
    v.nspname = ANY($1);
