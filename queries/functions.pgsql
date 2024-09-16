SELECT
    p.oid,
	TO_JSONB(JSON_OBJECT(
		'schema_name': quote_ident(pn.nspname),
		'local_name': quote_ident(p.proname)
	)) AS "name",
	p.prokind = 'p' AS is_procedure,
	p.pronargs AS input_arg_count,
	p.proargnames AS arg_names,
	pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
	pg_catalog.pg_get_function_result(p.oid) AS return_type,
	p.procost AS estimated_cost,
	NULLIF(p.prorows,0) AS estimated_rows,
	p.prosecdef AS "security",
	p.proleakproof AS is_leak_proof,
	p.proisstrict AS "strict",
	CASE p.provolatile
		WHEN 'i' THEN 'Immutable'
		WHEN 's' THEN 'Stable'
		WHEN 'v' THEN 'Volatile'
	END AS "behaviour",
	CASE p.proparallel
		WHEN 's' THEN 'Safe'
		WHEN 'r' THEN 'Restricted'
		WHEN 'u' THEN 'Unsafe'
	END AS "parallel",
	CASE pl.lanname
	    WHEN 'sql' THEN JSON_OBJECT(
	        'type': 'Sql',
	        'source': CASE
	            WHEN p.prosqlbody IS NOT NULL THEN pg_catalog.pg_get_function_sqlbody(p.oid)
	            ELSE p.prosrc
	        END,
	        'is_pre_parsed': p.prosqlbody IS NOT NULL
	    )
	    WHEN 'plpgsql' THEN JSON_OBJECT(
	        'type': 'Plpgsql',
	        'source': p.prosrc
        )
	    WHEN 'c' THEN JSON_OBJECT(
	        'type': 'C',
	        'name': p.prosrc,
	        'link_symbol': p.probin
	    )
	    WHEN 'internal' THEN JSON_OBJECT(
	        'type': 'Internal',
	        'name': p.prosrc
        )
	    ELSE JSON_OBJECT(
	        'type': 'Invalid',
	        'function_name': p.proname,
	        'language_name': pl.lanname
	    )
	END AS source_code,
	p.proconfig AS config,
	TO_JSONB(nd.dependencies || pd.dependencies || td.dependencies || tyd.dependencies) AS "dependencies"
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS pn
    ON p.pronamespace = pn.oid
JOIN pg_catalog.pg_language AS pl
    ON p.prolang = pl.oid
CROSS JOIN LATERAL (
    SELECT
        ARRAY[JSON_OBJECT(
            'schema_name': quote_ident(pn.nspname),
            'local_name': ''
        )] AS "dependencies"
) AS nd
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'schema_name': quote_ident(pdn.nspname),
            'local_name': quote_ident(pd.proname)
        )) AS "dependencies"
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
) AS pd
CROSS JOIN LATERAL (
	SELECT
	    ARRAY_AGG(JSON_OBJECT(
            'schema_name': quote_ident(td.nspname),
            'local_name': quote_ident(td.relname)
        )) AS "dependencies"
	FROM (
		SELECT DISTINCT td.relname, tdn.nspname
		FROM pg_catalog.pg_depend AS d
		JOIN pg_catalog.pg_class AS td
			ON d.refclassid = 'pg_class'::REGCLASS
			AND d.refobjid = td.oid
        JOIN pg_catalog.pg_namespace AS tdn
            ON td.relnamespace = tdn.oid
		WHERE
            d.classid = 'pg_proc'::REGCLASS
            AND d.objid = p.oid
			AND d.deptype = 'n'
			AND td.relkind IN ('r','p')
	) AS td
) AS td
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'schema_name': quote_ident(tyd.nspname),
            'local_name': quote_ident(tyd.typname)
        )) AS "dependencies"
    FROM (
        SELECT DISTINCT tyd.typname, tydn.nspname
        FROM pg_catalog.pg_depend AS d
        JOIN pg_catalog.pg_type AS tyd
            ON d.refclassid = 'pg_type'::REGCLASS
            AND d.refobjid = tyd.oid
        JOIN pg_catalog.pg_namespace AS tydn
            ON tyd.typnamespace = tydn.oid
        WHERE
            d.classid = 'pg_proc'::REGCLASS
            AND d.objid = p.oid
            AND d.deptype = 'n'
            AND
            (
                tyd.typtype IN ('e','r','d')
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
