create function test_func()
returns void
language plpgsql
as $$
declare
    test_int int := 1;
begin
    if test_int = 10 then
        test_int := test_int + 1;
    elsif test_int = 1 then
        test_int := test_int + 2;
    else
        test_int := test_int + 3;
    end if;
end;
$$;