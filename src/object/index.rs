use std::fmt::Write;

use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::PgDiffError;

use super::{
    compare_key_value_pairs, compare_tablespaces, IndexParameters, SchemaQualifiedName, SqlObject,
};

/// Fetch all indexes associated with the tables specified (as table OID)
pub async fn get_indexes(pool: &PgPool, tables: &[Oid]) -> Result<Vec<Index>, PgDiffError> {
    let indexes_query = include_str!("./../../queries/indexes.pgsql");
    let indexes = match query_as(indexes_query).bind(tables).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load index");
            return Err(error.into());
        },
    };
    Ok(indexes)
}

/// Struct representing a SQL index object
#[derive(Debug, sqlx::FromRow)]
pub struct Index {
    /// Owner table's OID
    pub(crate) table_oid: Oid,
    /// Owner table's full name
    #[sqlx(json)]
    pub(crate) owner_table_name: SchemaQualifiedName,
    /// Full name of the index where the local part starts with the table name followed by the index
    /// name to clarify the owner table
    #[sqlx(json)]
    pub(crate) schema_qualified_name: SchemaQualifiedName,
    /// Columns specified in the index
    pub(crate) columns: Vec<String>,
    /// Full SQL text of the index definition as found by `pg_catalog.pg_get_indexdef`
    pub(crate) definition_statement: String,
    /// Optional parameters of the index
    #[sqlx(flatten)]
    pub(crate) parameters: IndexParameters,
    /// Dependencies of the index. This is always just the owner table name
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        self.definition_statement == other.definition_statement
    }
}

impl SqlObject for Index {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "INDEX"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "{};", self.definition_statement)?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.columns == new.columns
            && self.parameters.include == new.parameters.include
            && self.parameters.with != new.parameters.with
        {
            compare_key_value_pairs(w, self, &self.parameters.with, &new.parameters.with, true)?;
            compare_tablespaces(
                self,
                self.parameters.tablespace.as_ref(),
                new.parameters.tablespace.as_ref(),
                w,
            )?;
            return Ok(());
        }

        self.drop_statements(w)?;
        self.create_statements(w)?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP INDEX {};", self.schema_qualified_name)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use sqlx::postgres::types::Oid;

    use crate::object::{IndexParameters, SchemaQualifiedName, SqlObject, TableSpace};

    use super::Index;

    const SCHEMA: &str = "test_schema";
    const TABLE: &str = "test_table";
    const NAME: &str = "test_index";
    const TABLESPACE_1: &str = "tbl_space";
    const TABLESPACE_2: &str = "other_tbl_space";
    const OPTION_1_1: &str = "fillfactor=100";
    const OPTION_1_2: &str = "fillfactor=90";
    const OPTION_2_1: &str = "buffering=ON";
    const OPTION_2_2: &str = "buffering=OFF";

    fn create_index(with: Option<Vec<&str>>, tablespace: Option<&str>) -> Index {
        Index {
            table_oid: Oid(1),
            owner_table_name: SchemaQualifiedName::new(SCHEMA, TABLE),
            schema_qualified_name: SchemaQualifiedName::new(SCHEMA, NAME),
            columns: vec![],
            definition_statement: String::from(""),
            parameters: IndexParameters {
                include: None,
                with: with.map(|w| w.as_slice().into()),
                tablespace: tablespace.map(|t| TableSpace(t.into())),
            },
            dependencies: vec![],
        }
    }

    #[rstest::rstest]
    #[case(
        create_index(
            None,
            None,
        ),
        create_index(
            Some(vec![OPTION_1_1, OPTION_2_1]),
            Some(TABLESPACE_1),
        ),
        include_str!("../../test-files/sql/index-alter-case1.pgsql"),
    )]
    #[case(
        create_index(
            Some(vec![OPTION_1_1, OPTION_2_1]),
            Some(TABLESPACE_1),
        ),
        create_index(
            Some(vec![OPTION_1_2, OPTION_2_2]),
            Some(TABLESPACE_2),
        ),
        include_str!("../../test-files/sql/index-alter-case2.pgsql"),
    )]
    #[case(
        create_index(
            Some(vec![OPTION_1_1, OPTION_2_1]),
            Some(TABLESPACE_1),
        ),
        create_index(
            None,
            None,
        ),
        include_str!("../../test-files/sql/index-alter-case3.pgsql"),
    )]
    #[case(
        create_index(
            Some(vec![OPTION_1_1]),
            None,
        ),
        create_index(
            Some(vec![OPTION_2_2]),
            None,
        ),
        include_str!("../../test-files/sql/index-alter-case4.pgsql"),
    )]
    fn alter_statements_should_add_alter_index_statement(
        #[case] old: Index,
        #[case] new: Index,
        #[case] statement: &str,
    ) {
        let mut writeable = String::new();

        old.alter_statements(&new, &mut writeable).unwrap();

        assert_eq!(statement.trim(), writeable.trim());
    }
}
