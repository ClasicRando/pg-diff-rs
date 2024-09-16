use std::fmt::Write;

use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::PgDiffError;

use super::{
    compare_option_lists, compare_tablespaces, IndexParameters, OptionListObject,
    SchemaQualifiedName, SqlObject,
};

/// Fetch all indexes associated with the tables specified (as table OID)
pub async fn get_indexes(pool: &PgPool, tables: &[Oid]) -> Result<Vec<Index>, PgDiffError> {
    let indexes_query = include_str!("./../../queries/indexes.pgsql");
    let indexes = match query_as(indexes_query).bind(tables).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load index");
            return Err(error.into());
        }
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

impl OptionListObject for Index {}

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
            compare_option_lists(
                self,
                self.parameters.with.as_deref(),
                new.parameters.with.as_deref(),
                w,
            )?;
            compare_tablespaces(
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
