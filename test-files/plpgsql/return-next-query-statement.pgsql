create function test_func()
returns setof int
language plpgsql
as $$
begin
    return next 1;
    return query select 1;
    return query execute 'select $1' using 1;
    return;
end;
$$;