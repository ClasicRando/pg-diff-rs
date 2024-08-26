use std::fmt::Write;

use sqlx::postgres::PgRow;
use sqlx::{query_as, FromRow, PgPool, Row};
use sqlx::postgres::types::Oid;

use crate::PgDiffError;

use super::{Dependency, PgCatalog, SchemaQualifiedName, SqlObject};

pub async fn get_schemas(pool: &PgPool) -> Result<Vec<Schema>, PgDiffError> {
    let schemas_query = include_str!("./../../queries/schemas.pgsql");
    let schema_names = match query_as(schemas_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load schemas");
            return Err(error.into());
        }
    };
    Ok(schema_names)
}

#[derive(Debug, PartialEq)]
pub struct Schema {
    pub(crate) oid: Oid,
    pub(crate) name: SchemaQualifiedName,
    pub(crate) owner: String,
}

impl<'r> FromRow<'r, PgRow> for Schema {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let oid: Oid = row.try_get("oid")?;
        let name: String = row.try_get("name")?;
        let owner: String = row.try_get("owner")?;
        Ok(Self {
            oid,
            name: SchemaQualifiedName {
                local_name: "".to_string(),
                schema_name: name,
            },
            owner,
        })
    }
}

impl SqlObject for Schema {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "SCHEMA"
    }

    fn dependency_declaration(&self) -> Dependency {
        Dependency {
            oid: self.oid,
            catalog: PgCatalog::Namespace,
        }
    }
    
    fn dependencies(&self) -> &[Dependency] {
        &[]
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(
            w,
            "CREATE SCHEMA {} AUTHORIZATION {};",
            self.name, self.owner
        )?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "ALTER SCHEMA {} OWNER TO {};", self.name, new.owner)?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP SCHEMA {};", self.name)?;
        Ok(())
    }

    fn dependencies_met(&self, _: &[Dependency]) -> bool {
        true
    }
}
