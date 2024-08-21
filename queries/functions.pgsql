SELECT
	TO_JSONB(JSON_OBJECT(
		'schema_name': quote_ident(pn.nspname),
		'local_name': quote_ident(p.proname)
	)) AS "name",
	p.prokind = 'p' AS is_procedure,
	pg_catalog.pg_get_function_identity_arguments(p.oid) AS "signature",
	pg_catalog.pg_get_functiondef(p.oid) AS "definition",
	pl.lanname AS "language",
	TO_JSONB(pd."function_dependencies") AS "function_dependencies"
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS pn
    ON p.pronamespace = pn.oid
JOIN pg_catalog.pg_language AS pl
    ON p.prolang = pl.oid
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'name': JSON_OBJECT(
                'schema_name': quote_ident(pn.nspname),
                'local_name': quote_ident(p.proname)
            ),
            'signature': pg_catalog.pg_get_function_identity_arguments(pd.oid)
        )) AS "function_dependencies"
    FROM pg_catalog.pg_depend AS d
    JOIN pg_catalog.pg_proc AS pd
        ON d.refclassid = 'pg_proc'::REGCLASS
        AND d.refobjid = pd.oid
    JOIN pg_catalog.pg_namespace AS pdn
        ON pd.pronamespace = pdn.oid
    WHERE
        d.classid = 'pg_proc'::REGCLASS
        AND d.objid = p.oid
        AND d.deptype = 'n'
) pd
WHERE
    pn.nspname = ANY($1)
    AND p.prokind IN ('f','p')
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT NULL
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_proc'::REGCLASS
            AND d.objid = p.oid
            AND d.deptype = 'e'
    );