WITH roles AS (
    SELECT r.oid, r.rolname
    FROM pg_catalog.pg_roles r
    UNION
    SELECT 0 AS "oid", 'PUBLIC' AS role_name
)
SELECT
    pol.oid,
    pol.polrelid AS table_oid,
    pol.polname AS name,
    JSON_OBJECT(
        'schema_name': quote_ident(tn.nspname),
        'local_name': quote_ident(pol.polname)
    ) AS schema_qualified_name,
    JSON_OBJECT(
        'schema_name': quote_ident(tn.nspname),
        'local_name': quote_ident(t.relname)
    ) AS owner_table_name,
    pol.polpermissive AS is_permissive,
    (
        SELECT ARRAY_AGG(rolname)
        FROM roles AS r
        WHERE r.oid = ANY(pol.polroles)
    ) AS applies_to,
    CASE pol.polcmd
        WHEN 'r' THEN 'Select'
        WHEN 'a' THEN 'Insert'
        WHEN 'w' THEN 'Update'
        WHEN 'd' THEN 'Delete'
        WHEN '*' THEN 'All'
    END AS command,
    pg_catalog.pg_get_expr(
        pol.polwithcheck,
        pol.polrelid
    ) AS check_expression,
    pg_catalog.pg_get_expr(
        pol.polqual,
        pol.polrelid
    ) AS using_expression,
    COALESCE(c.columns, '{}') AS "columns",
    TO_JSONB(COALESCE(td.dependencies || pd.dependencies || tyd.dependencies, '{}')) AS "dependencies"
FROM pg_catalog.pg_policy AS pol
JOIN pg_catalog.pg_class AS t
    ON pol.polrelid = t.oid
JOIN pg_catalog.pg_namespace AS tn
    ON t.relnamespace = tn.oid
CROSS JOIN LATERAL (
   SELECT ARRAY_AGG(a.attname ORDER BY a.attnum) AS "columns"
   FROM pg_catalog.pg_attribute AS a
   INNER JOIN pg_catalog.pg_depend AS d
       ON a.attnum = d.refobjsubid
   WHERE
       d.objid = pol.oid
       AND d.refobjid = t.oid
       AND d.refclassid = 'pg_class'::REGCLASS
       AND a.attrelid = t.oid
       AND NOT a.attisdropped
       AND a.attnum > 0
) AS c
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
            d.classid = 'pg_policy'::REGCLASS
            AND d.objid = pol.oid
			AND d.deptype = 'n'
			AND td.relkind IN ('r','p')
	) AS td
) AS td
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'catalog': 'pg_proc',
            'oid': CAST(pd.oid AS integer)
        )) AS "dependencies"
    FROM pg_catalog.pg_depend AS d
    JOIN pg_catalog.pg_proc AS pd
        ON d.refclassid = 'pg_proc'::REGCLASS
        AND d.refobjid = pd.oid
    WHERE
        d.classid = 'pg_policy'::REGCLASS
        AND d.objid = pol.oid
        AND d.deptype = 'n'
) AS pd
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'catalog': 'pg_type',
            'oid': CAST(tyd.oid AS integer)
        )) AS "dependencies"
    FROM (
        SELECT DISTINCT tyd.oid
        FROM pg_catalog.pg_depend AS d
        JOIN pg_catalog.pg_type AS tyd
            ON d.refclassid = 'pg_type'::REGCLASS
            AND d.refobjid = tyd.oid
        WHERE
            d.classid = 'pg_policy'::REGCLASS
            AND d.objid = pol.oid
            AND d.deptype = 'n'
            AND
            (
                tyd.typtype IN ('e','r')
                OR
                (
                    tyd.typtype = 'c'
                    AND EXISTS(
                        SELECT NULL
                        FROM pg_catalog.pg_class tc
                        WHERE
                            tc.oid = tyd.typrelid
                            AND tc.relkind = 'c'
                    )
                )
            )
    ) AS tyd
) AS tyd
WHERE
    pol.polrelid = ANY($1);