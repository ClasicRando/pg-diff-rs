create function test_func()
returns void
language plpgsql
as $$
begin
    commit and chain;
    rollback and chain;
end;
$$;