create function test_func()
returns void
language plpgsql
as $$
declare
    cur1 cursor(id integer) for select id as "id";
    cur2 cursor for select 1 as "id";
    test_int int;
begin
    for r_row in cur1 (id := 1)
    loop
        test_int := test_int + r_row.id;
    end loop;
    for r_row in cur2
    loop
        test_int := test_int + r_row.id;
    end loop;
end;
$$;