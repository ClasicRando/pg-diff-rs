SELECT r.rolcreatedb
FROM pg_roles r
WHERE r.rolname = CURRENT_USER;
