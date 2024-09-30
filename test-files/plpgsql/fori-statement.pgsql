create function test_func()
returns void
language plpgsql
as $$
declare
    test_int int;
    i int;
begin
    for i in 1..10
    loop
        test_int := test_int + i;
    end loop;
    for i in 1..10 by 2
    loop
        test_int := test_int + i;
    end loop;
    for i in reverse 10..1
    loop
        test_int := test_int + i;
    end loop;
end;
$$;