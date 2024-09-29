create function test_func()
returns void
language plpgsql
as $$
declare
    test_int int;
    curs1 refcursor;
    curs2 refcursor;
    curs3 cursor(id integer) for select id + 1; 
begin
    open curs1 no scroll for select 1 as "id";
    close curs1;

    open curs2 scroll for execute 'select $1 as "id"' using 1;
    fetch next from curs2 into test_int;
    move prior from curs2;
    fetch first from curs2 into test_int;
    move prior from curs2;
    fetch last from curs2 into test_int;
    move backward from curs2;
    fetch absolute 1 from curs2 into test_int;
    move backward from curs2;
    fetch relative 1 from curs2 into test_int;
    move backward from curs2;
    fetch forward from curs2 into test_int;
    close curs2;

    open curs3(1);
    close curs3;
end;
$$;