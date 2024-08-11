WITH table_columns AS (
	SELECT
		a.attrelid,
		json_object(
			'name': a.attname,
			'data_type': t.typname,
			'size': a.attlen,
			'collation': '"'||cn.nspname||'"."'||cl.collname||'"',
			'is_non_null': attnotnull,
			'default_expression': CASE
			    WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(def.adbin, def.adrelid)
			END,
			'generated_column': CASE
			    WHEN a.attgenerated = 's' THEN
			        json_object(
			            'type': 'Stored',
			            'expression': pg_catalog.pg_get_expr(def.adbin, def.adrelid)
			        )
			END,
			'identity_column': CASE
			    WHEN a.attidentity = 'a' THEN 'Always'
			    WHEN a.attidentity = 'd' THEN 'Default'
			END
		) AS "json_object"
	FROM pg_catalog.pg_attribute a
	JOIN pg_catalog.pg_type t
	    ON a.atttypid = t.oid
	LEFT JOIN pg_catalog.pg_collation cl
	    ON a.attcollation = cl.oid
	LEFT JOIN pg_catalog.pg_namespace cn
	    ON cl.collnamespace = cn.oid
	LEFT JOIN pg_catalog.pg_attrdef def
	    ON a.attrelid = def.adrelid
	    AND a.attnum = def.adnum
	WHERE
	    a.attnum > 0
	    AND NOT a.attisdropped
), roles AS (
    SELECT oid, rolname
    FROM pg_catalog.pg_roles
    UNION
    SELECT 0 AS "oid", 'PUBLIC' AS role_name
)
SELECT
	to_jsonb(json_object(
		'schema_name': tn.nspname,
		'local_name': t.relname
	)) AS "name",
	(
		SELECT to_jsonb(ARRAY_AGG(c."json_object"))
		FROM table_columns c
		WHERE t.oid = c.attrelid
	) AS "columns",
	(
		SELECT
			to_jsonb(ARRAY_AGG(json_object(
				'name': pol.polname,
				'is_permissive': pol.polpermissive,
				'applies_to': (
					SELECT ARRAY_AGG(rolname)
					FROM roles
					WHERE roles.oid = ANY(pol.polroles)
				),
				'command': pol.polcmd,
				'check_expression': pg_catalog.pg_get_expr(
					pol.polwithcheck,
					pol.polrelid
				),
				'using_expression': pg_catalog.pg_get_expr(
					pol.polqual,
					pol.polrelid
				),
				'columns': (
					SELECT ARRAY_AGG(a.attname)
					FROM pg_catalog.pg_attribute AS a
					INNER JOIN pg_catalog.pg_depend AS d ON a.attnum = d.refobjsubid
					WHERE
						d.objid = pol.oid
						AND d.refobjid = t.oid
						AND d.refclassid = 'pg_class'::REGCLASS
						AND a.attrelid = t.oid
						AND NOT a.attisdropped
						AND a.attnum > 0
				)
			)))
		FROM pg_catalog.pg_policy as pol
		WHERE t.oid = pol.polrelid
	) AS "policies",
	CASE
        WHEN t.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(t.oid)
        ELSE NULL
    END AS "partition_key_def",
	CASE
		WHEN pt.oid IS NOT NULL THEN
			to_jsonb(json_object(
				'schema_name': pn.nspname,
				'local_name': pt.relname
			))
		ELSE NULL
	END AS "parent_table",
	CASE
        WHEN t.relispartition THEN pg_catalog.pg_get_expr(t.relpartbound, t.oid)
        ELSE NULL
    END AS "partition_values"
FROM pg_catalog.pg_class AS t
JOIN pg_catalog.pg_namespace AS tn
	ON t.relnamespace = tn.oid
LEFT JOIN pg_catalog.pg_inherits AS p
	ON t.oid = p.inhrelid
LEFT JOIN pg_catalog.pg_class AS pt
    ON p.inhparent = pt.oid
LEFT JOIN pg_catalog.pg_namespace AS pn
    ON pt.relnamespace = pn.oid
WHERE
    tn.nspname = $1
	AND t.relkind IN ('r','p')
	AND tn.nspname NOT IN ('pg_catalog','information_schema')
    AND tn.nspname !~ '^pg_toast'
    AND tn.nspname !~ '^pg_temp'
    -- Exclude tables owned by extensions
    AND NOT EXISTS (
        SELECT d.objid
        FROM pg_catalog.pg_depend AS d
        WHERE
            d.classid = 'pg_class'::REGCLASS
            AND d.objid = t.oid
            AND d.deptype = 'e'
    );