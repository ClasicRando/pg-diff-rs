create function test_func()
language plpgsql
as $$
declare
    test_int int;
begin
    <<ablock>>
    loop
        test_int := test_int + 1;
        continue ablock when test_int < 5;
        exit ablock when test_int >= 10;
        continue ablock;
    end loop ablock;
    
    loop
        test_int := test_int + 1;
        continue when test_int < 5;
        exit when test_int >= 10;
        continue;
    end loop;
end;
$$;