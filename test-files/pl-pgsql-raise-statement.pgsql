create function test_func()
returns void
language plpgsql
as $$
begin
    raise exception using
        message = 'Message',
        detail = 'Detail',
        hint = 'Hint',
        errcode = 'unique_violation',
        column = 'column',
        constraint = 'constraint',
        datatype = 'datatype',
        table = 'table',
        schema = 'schema';
    raise warning 'This will not happen %s', 'test' using detail = 'Detail';
    raise notice 'This will not happen %s', 'test';
    raise info division_by_zero;
    raise log division_by_zero;
    raise debug division_by_zero;
end;
$$;