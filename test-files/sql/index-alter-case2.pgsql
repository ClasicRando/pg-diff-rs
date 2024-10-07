ALTER INDEX test_schema.test_index SET (buffering=OFF,fillfactor=90);
ALTER INDEX test_schema.test_index SET TABLESPACE other_tbl_space;