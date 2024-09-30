create function test_func()
returns void
language plpgsql
as $$
declare
    test_int int := 0;
begin
    assert test_int != 0, 'Assertion failed';
    assert test_int != 0;
end;
$$;