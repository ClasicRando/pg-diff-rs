WITH extensions AS (
    SELECT
        e.oid,
        e.extname,
        e.extversion,
        n.nspname,
        e.extrelocatable
    FROM pg_catalog.pg_extension AS e
    JOIN pg_catalog.pg_namespace AS n
        ON e.extnamespace = n.oid
)
SELECT
    e.oid,
    JSON_OBJECT(
        'schema_name': '',
        'local_name': quote_ident(e.extname)
    ) AS "name",
    e.extversion AS "version",
    e.nspname AS "schema_name",
    e.extrelocatable AS is_relocatable,
    TO_JSONB(
        ed.dependencies ||
        CASE
            WHEN e.extrelocatable AND e.nspname NOT IN ('public')
                THEN ARRAY[JSON_OBJECT('schema_name': quote_ident(e.nspname), 'local_name': '')]
            ELSE '{}'::json[]
        END
    ) AS "dependencies"
FROM extensions AS e
CROSS JOIN LATERAL (
	SELECT ARRAY_AGG(JSON_OBJECT(
	    'schema_name': '',
	    'local_name': quote_ident(ed.extname)
    )) AS "dependencies"
	FROM (
		SELECT DISTINCT ed.extname
		FROM pg_catalog.pg_depend AS d
		JOIN extensions AS ed
			ON d.refclassid = 'pg_extension'::REGCLASS
			AND d.refobjid = ed.oid
		WHERE
			d.classid = 'pg_extension'::REGCLASS
			AND d.objid = e.oid
			AND d.deptype IN ('x')
	) AS ed
) AS ed
WHERE
    e.nspname NOT IN ('pg_catalog', 'information_schema')
    AND e.nspname !~ '^pg_toast'
    AND e.nspname !~ '^pg_temp';