use std::fmt::Write;

use sqlx::postgres::PgRow;
use sqlx::{query_as, FromRow, PgPool, Row};

use crate::PgDiffError;

use super::util::{SchemaQualifiedName, SqlObject};

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
    pub(crate) name: SchemaQualifiedName,
    pub(crate) owner: String,
}

impl<'r> FromRow<'r, PgRow> for Schema {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let name: String = row.try_get("name")?;
        let owner: String = row.try_get("owner")?;
        Ok(Self {
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
}
