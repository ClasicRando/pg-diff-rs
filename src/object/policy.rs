use std::fmt::Write;

use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::PgDiffError;

use super::{SchemaQualifiedName, SqlObject};

pub async fn get_policies(pool: &PgPool, schemas: &[Oid]) -> Result<Vec<Policy>, PgDiffError> {
    let tables_query = include_str!("./../../queries/policies.pgsql");
    let tables = match query_as(tables_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load policies");
            return Err(error.into());
        },
    };
    Ok(tables)
}

#[derive(Debug, sqlx::FromRow)]
pub struct Policy {
    pub(crate) table_oid: Oid,
    pub(crate) name: String,
    #[sqlx(json)]
    pub(crate) schema_qualified_name: SchemaQualifiedName,
    #[sqlx(json)]
    pub(crate) owner_table_name: SchemaQualifiedName,
    pub(crate) is_permissive: bool,
    pub(crate) applies_to: Vec<String>,
    pub(crate) command: PolicyCommand,
    pub(crate) check_expression: Option<String>,
    pub(crate) using_expression: Option<String>,
    pub(crate) columns: Vec<String>,
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for Policy {
    #[inline]
    fn eq(&self, other: &Policy) -> bool {
        self.name == other.name
            && self.schema_qualified_name == other.schema_qualified_name
            && self.owner_table_name == other.owner_table_name
            && self.is_permissive == other.is_permissive
            && self.applies_to == other.applies_to
            && self.command == other.command
            && self.check_expression == other.check_expression
            && self.using_expression == other.using_expression
            && self.columns == other.columns
            && self.dependencies == other.dependencies
    }
}

impl SqlObject for Policy {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "POLICY"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(
            w,
            "CREATE POLICY {}\n    ON {}\n    AS {}\n    FOR {}\n    TO {}",
            self.name,
            self.owner_table_name,
            if self.is_permissive {
                "PERMISSIVE"
            } else {
                "RESTRICTIVE"
            },
            self.command.as_ref(),
            self.applies_to.join(" ")
        )?;
        if let Some(using_expression) = &self.using_expression {
            write!(w, "\n    USING ({using_expression})")?;
        }
        if let Some(check_expression) = &self.check_expression {
            write!(w, "\n    WITH CHECK ({check_expression})")?;
        }
        w.write_str(";\n")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        println!("{:?}", self);
        println!("{new:?}");
        if self.is_permissive != new.is_permissive || self.command != new.command {
            self.drop_statements(w)?;
            self.create_statements(w)?;
            return Ok(());
        }
        write!(
            w,
            "ALTER POLICY {}\n    ON {}\n    TO {}",
            self.name,
            self.owner_table_name,
            new.applies_to.join(" ")
        )?;
        if let Some(using_expression) = &new.using_expression {
            write!(w, "\n    USING ({using_expression})")?;
        }
        if let Some(check_expression) = &new.check_expression {
            write!(w, "\n    WITH CHECK ({check_expression})")?;
        }
        w.write_str(";\n")?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP POLICY {} ON {};", self.name, self.owner_table_name)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, sqlx::Type, strum::AsRefStr)]
#[sqlx(type_name = "text")]
pub enum PolicyCommand {
    #[strum(serialize = "SELECT")]
    Select,
    #[strum(serialize = "INSERT")]
    Insert,
    #[strum(serialize = "UPDATE")]
    Update,
    #[strum(serialize = "DELETE")]
    Delete,
    #[strum(serialize = "ALL")]
    All,
}
