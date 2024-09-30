create function test_func()
returns void
language plpgsql
as $$
declare
    test_int int;
    arr int[];
    i int;
begin
    foreach arr slice 1 in array array[array[1,2,3,4],array[5,6,7,8]]
    loop
        foreach i in array arr
        loop
            test_int := test_int + i;
        end loop;
    end loop;
end;
$$;