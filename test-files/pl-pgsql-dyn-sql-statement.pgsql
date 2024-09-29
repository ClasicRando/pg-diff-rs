create function test_func()
returns void
language plpgsql
as $$
declare
    oid_var oid;
    text_var text;
begin
    execute 'select oid from pg_catalog.pg_type offset 0 fetch first row only'
    into strict oid_var;
    
    execute 'select oid, typname from pg_catalog.pg_type offset 0 fetch first row only'
    into oid_var, text_var;
    
    execute 'update test_table set test_field = $1 where id = $2'
    using 'test', 1;
end;
$$;