create function test_func()
language plpgsql
as $$
declare
    test_int int;
begin
    test_int := test_int + 1;
end;
$$;