create function test_func()
returns void
language plpgsql
as $$
declare
    test_int int;
    i record;
begin
    for i in (select 1 as "id")
    loop
        test_int := test_int + i.id;
    end loop;
end;
$$;