create function test_func()
returns void
language plpgsql
as $$
declare
    test_int int;
begin
    while test_int <= 15
    loop
        test_int := test_int + 1;
    end loop;
end;
$$;