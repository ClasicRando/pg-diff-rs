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
	pl.lanname AS "language",
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
	CASE
		WHEN pl.lanname = 'sql' AND p.prosqlbody IS NOT NULL THEN pg_catalog.pg_get_function_sqlbody(p.oid)
		WHEN pl.lanname IN ('sql','plpgsql','c','internal') THEN prosrc
		ELSE NULL
	END AS "source",
	p.probin AS bin_info,
	p.proconfig AS config,
	p.prosqlbody IS NOT NULL AS is_pre_parsed,
	TO_JSONB(nd.dependencies || pd.dependencies || td.dependencies || tyd.dependencies) AS "dependencies"
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS pn
    ON p.pronamespace = pn.oid
JOIN pg_catalog.pg_language AS pl
    ON p.prolang = pl.oid
CROSS JOIN LATERAL (
    SELECT
        ARRAY[JSON_OBJECT(
            'oid': CAST(pn.oid AS INTEGER),
            'catalog': 'pg_namespace'
        )] AS "dependencies"
) AS nd
CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(JSON_OBJECT(
            'oid': CAST(pd.oid AS INTEGER),
            'catalog': 'pg_proc'
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
            d.classid = 'pg_proc'::REGCLASS
            AND d.objid = p.oid
			AND d.deptype = 'n'
			AND td.relkind IN ('r','p')
	) AS td
) AS td
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
            d.classid = 'pg_proc'::REGCLASS
            AND d.objid = p.oid
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
