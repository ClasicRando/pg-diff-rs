SELECT
    pg_catalog.pg_encoding_to_char(d.encoding) AS "encoding",
    CASE d.datlocprovider
        WHEN 'c' THEN JSON_OBJECT(
            'type': d.datlocprovider,
            'lc_collate': d.datcollate,
            'lc_ctype': d.datctype
        )
        WHEN 'i' THEN JSON_OBJECT(
            'type': d.datlocprovider,
            'icu_locale': d.daticulocale,
            'icu_rules': d.daticurules
        )
    END AS "locale_provider",
    ts.spcname AS "tablespace",
    COALESCE(d.datcollate, d.datctype, d.daticulocale) AS "locale",
    d.datcollversion AS "collation_version"
FROM pg_catalog.pg_database AS d
JOIN pg_catalog.pg_tablespace AS ts
    ON d.dattablespace = ts.oid
WHERE
    d.datname = CURRENT_DATABASE();