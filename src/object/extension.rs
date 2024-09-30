use std::fmt::Write;

use sqlx::{query_as, PgPool};

use crate::PgDiffError;

use super::{SchemaQualifiedName, SqlObject};

/// Fetch all extensions found within the current database
pub async fn get_extensions(pool: &PgPool) -> Result<Vec<Extension>, PgDiffError> {
    let extensions_query = include_str!("./../../queries/extensions.pgsql");
    let extensions = match query_as(extensions_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load extensions");
            return Err(error.into());
        },
    };
    Ok(extensions)
}

/// Postgresql extension object
#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Extension {
    /// Full name of the extension (never includes a schema name since extensions reside outside a
    /// schema even though the objects owned by the extension are within a schema)
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    /// Version of the extension
    pub(crate) version: String,
    /// Schema where the extension resides
    pub(crate) schema_name: String,
    /// True if the extension allows relocating the extension objects into a user defined schema
    pub(crate) is_relocatable: bool,
    /// Dependencies of the schema. This is only ever populated with other extensions this extension
    /// depends upon and/or the schema that this extension is located within if it's not public and
    /// is relocatable.
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl SqlObject for Extension {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "EXTENSION"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(
            w,
            "CREATE EXTENSION {} VERSION '{}'",
            self.name, self.version
        )?;
        if self.is_relocatable {
            write!(w, " SCHEMA {}", self.schema_name)?;
        }
        w.write_str(";\n")?;
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
                "ALTER EXTENSION {} UPDATE TO '{}';",
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
