SELECT
    e.oid,
    JSON_OBJECT(
        'schema_name': '',
        'local_name': e.extname
    ) AS "name",
    e.extversion AS "version",
    n.nspname AS "schema_name",
    e.extrelocatable AS is_relocatable,
    TO_JSONB(
        ed.dependencies ||
        CASE
            WHEN e.extrelocatable AND n.nspname NOT IN ('public')
                THEN ARRAY[JSON_OBJECT('catalog': 'pg_namespace', 'oid': CAST(n.oid AS integer))]
            ELSE '{}'::json[]
        END
    ) AS "dependencies"
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n
    ON e.extnamespace = n.oid
CROSS JOIN LATERAL (
	SELECT ARRAY_AGG(JSON_OBJECT(
		'catalog': 'pg_extension',
		'oid': CAST(ed.oid AS integer)
	)) AS "dependencies"
	FROM (
		SELECT DISTINCT ed.oid
		FROM pg_catalog.pg_depend AS d
		JOIN pg_catalog.pg_extension AS ed
			ON d.refclassid = 'pg_extension'::REGCLASS
			AND d.refobjid = ed.oid
		WHERE
			d.classid = 'pg_extension'::REGCLASS
			AND d.objid = e.oid
			AND d.deptype IN ('x')
	) AS ed
) AS ed
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname !~ '^pg_toast'
    AND n.nspname !~ '^pg_temp';