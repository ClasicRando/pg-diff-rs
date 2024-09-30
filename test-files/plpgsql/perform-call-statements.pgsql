create function test_func()
returns void
language plpgsql
as $$
declare
    i int;
begin
    perform pg_notify('','');
    call test_procedure();
    do $do$update test set fld = true;$do$;
end;
$$;