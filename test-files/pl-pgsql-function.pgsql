create function test_func()
language plpgsql
as $$
declare
    test_int int;
    i int;
    arr int[];
    cur1 cursor(id integer) for select id;
    cur2 refcursor;
    cur3 cursor(id integer) for select id + 1; 
    oid_var oid;
    r_dyn record;
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
begin;
    test_int := (select 1);
    if test_int = 10 then
        raise error using
            message = 'Message',
            detail = 'Detail',
            hint = 'Hint',
            errcode = 'unique_violation',
            column = 'column',
            constraint = 'constraint',
            datatype = 'datatype',
            table = 'table',
            schema = 'schema';
    elsif test_int = 5 then
        raise warn 'This will not happen %s', 'test' using detail = 'Detail';
    elsif test_int = 4 then
        raise notice 'This will not happen %s', 'test';
    elsif test_int = 3 then
        raise info division_by_zero;
        raise log division_by_zero;
        raise debug division_by_zero;
    elsif test_int = 2 then
        test_int :=
            case test_int
                when 1 then 2
                when 2 then (select test_int + 1)
                else null
            end;
    elsif test_int = 1 then
        call test_procedure;
    else
        perform select 1;
    end if;
    
    assert test_int != 0, 'Assertion failed';

    <<ablock>>
    loop
        test_int := test_int + 1;
        continue ablock when test_int < 5;
        exit ablock when test_int >= 10;
    end loop ablock;
    
    <<bblock>>
    while test_int <= 15
    loop
        test_int := test_int + 1;
    end loop bblock;
    
    <<cblock>>
    for counter in reverse 10..1
    loop
        test_int := test_int + counter;
    end loop cblock;
    
    <<dblock>>
    for i in (select 1)
    loop
        test_int := test_int + i;
    end loop dblock;
    
    <<eblock>>
    for r_row in cur1 (id := 1)
    loop
        test_int := test_int + r_row.id;
    end loop eblock;
    
    <<fblock>>
    foreach arr slice 1 in array array[array[1,2,3,4],array[5,6,7,8]]
    loop
        <<gblock>>
        foreach i in array arr
        loop
            test_int := test_int + i;
        end loop gblock;
    end loop fblock;
    
    <<hblock>>
    for r_dyn in execute 'select $1 as "id"' using 1;
    loop
        test_int := test_int + r_dyn.id;
    end loop hblock;
    
    select oid
    into strict oid_var
    from pg_catalog.pg_type
    offset 0
    fetch first row only;
    
    execute 'select count(*) from (select 1 as "id") t where t.id = $1'
    into strict test_int
    using 1;
    
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
        
    open cur2 no scroll for select 1 as "id";
    close cur2;
    
    open cur2 scroll for execute 'select $1 as "id"' using 1;
    fetch next cur2 into test_int;
    move prior cur2;
    fetch first cur2 into test_int;
    move prior cur2;
    fetch last cur2 into test_int;
    move backward cur2;
    fetch absolute 1 cur2 into test_int;
    move backward cur2;
    fetch relative 1 cur2 into test_int;
    move backward cur2;
    fetch forward cur2 into test_int;
    close cur2;
    
    open cur3(1);
    close cur3;
    commit and chain;
    rollback and chain;
end;
$$;