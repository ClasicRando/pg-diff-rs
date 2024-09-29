create function test_func()
returns void
language plpgsql
as $$
declare
    oid_var oid;
    text_var text;
begin
    select oid
    into strict oid_var
    from pg_catalog.pg_type
    offset 0
    fetch first row only;
    
    select oid, typname
    into oid_var, text_var
    from pg_catalog.pg_type
    offset 0
    fetch first row only;
    
    update test_table
    set test_field = 'test'
    where id = 1;
end;
$$;