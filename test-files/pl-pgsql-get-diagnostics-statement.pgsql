create function test_func()
returns void
language plpgsql
as $$
declare
    sql_state text;
    col_name text;
    con_name text;
    pg_datatype_name text;
    m_text text;
    t_name text;
    s_name text;
    e_detail text;
    e_hint text;
    e_context text;
begin
    get stacked diagnostics
        sql_state := RETURNED_SQLSTATE,
        col_name := COLUMN_NAME,
        con_name := CONSTRAINT_NAME,
        pg_datatype_name := PG_DATATYPE_NAME,
        m_text := MESSAGE_TEXT,
        t_name := TABLE_NAME,
        s_name := SCHEMA_NAME,
        e_detail := PG_EXCEPTION_DETAIL,
        e_hint := PG_EXCEPTION_HINT,
        e_context := PG_EXCEPTION_CONTEXT;
end;
$$;