use std::fmt::Write;

use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::PgDiffError;

use super::{
    compare_option_lists, IndexParameters, OptionListObject, SchemaQualifiedName, SqlObject,
    TablespaceCompare,
};

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

#[derive(Debug, sqlx::FromRow)]
pub struct Index {
    pub(crate) table_oid: Oid,
    #[sqlx(json)]
    pub(crate) owner_table_name: SchemaQualifiedName,
    #[sqlx(json)]
    pub(crate) schema_qualified_name: SchemaQualifiedName,
    pub(crate) columns: Vec<String>,
    pub(crate) definition_statement: String,
    #[sqlx(flatten)]
    pub(crate) parameters: IndexParameters,
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
            let compare_tablespace = TablespaceCompare::new(
                self.parameters.tablespace.as_ref(),
                new.parameters.tablespace.as_ref(),
            );
            if compare_tablespace.has_diff() {
                writeln!(
                    w,
                    "ALTER INDEX {} {compare_tablespace};",
                    self.schema_qualified_name,
                )?;
            }
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
