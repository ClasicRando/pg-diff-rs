use std::fmt::Write;

use sqlx::postgres::PgRow;
use sqlx::{Error, FromRow, PgPool, query_as, Row};

use crate::object::util::{SchemaQualifiedName, SqlObject};
use crate::PgDiffError;

pub async fn get_extensions(pool: &PgPool) -> Result<Vec<Extension>, PgDiffError> {
    let extensions_query = include_str!("./../../queries/extensions.pgsql");
    let extensions = match query_as(extensions_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load extensions");
            return Err(error.into());
        }
    };
    Ok(extensions)
}

#[derive(Debug, PartialEq)]
pub struct Extension {
    pub(crate) name: SchemaQualifiedName,
    pub(crate) version: String,
    pub(crate) schema_name: String,
    pub(crate) is_relocatable: bool,
}

impl<'r> FromRow<'r, PgRow> for Extension {
    fn from_row(row: &'r PgRow) -> Result<Self, Error> {
        let name: String = row.try_get("name")?;
        let version: String = row.try_get("version")?;
        let schema_name: String = row.try_get("schema_name")?;
        let is_relocatable: bool = row.try_get("is_relocatable")?;
        Ok(Self {
            name: SchemaQualifiedName {
                local_name: name,
                schema_name: "".to_string(),
            },
            version,
            schema_name,
            is_relocatable,
        })
    }
}

impl SqlObject for Extension {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "EXTENSION"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE EXTENSION {} VERSION {}", self.name, self.version)?;
        if self.is_relocatable {
            write!(w, " SCHEMA {}", self.schema_name)?;
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.schema_name != new.schema_name && self.is_relocatable {
            writeln!(
                w,
                "ALTER EXTENSION {} SET SCHEMA {};",
                self.name, new.schema_name
            )?;
        }
        if self.version != new.version {
            writeln!(
                w,
                "ALTER EXTENSION {} UPDATE TO {};",
                self.name, new.version
            )?;
        }
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP EXTENSION {};", self.name)?;
        Ok(())
    }
}
