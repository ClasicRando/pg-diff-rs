create function test_func()
returns int
language plpgsql
as $$
begin
    return 1;
end;
$$;