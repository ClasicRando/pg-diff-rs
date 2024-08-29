use std::fmt::Write;

use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::PgDiffError;

use super::{Dependency, PgCatalog, SchemaQualifiedName, SqlObject};

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

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Extension {
    pub(crate) oid: Oid,
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    pub(crate) version: String,
    pub(crate) schema_name: String,
    pub(crate) is_relocatable: bool,
    #[sqlx(json)]
    pub(crate) dependencies: Vec<Dependency>,
}

// impl<'r> FromRow<'r, PgRow> for Extension {
//     fn from_row(row: &'r PgRow) -> Result<Self, Error> {
//         let oid: Oid = row.try_get("oid")?;
//         let name: String = row.try_get("name")?;
//         let version: String = row.try_get("version")?;
//         let schema_name: String = row.try_get("schema_name")?;
//         let is_relocatable: bool = row.try_get("is_relocatable")?;
//         let dependencies: Json<Vec<Dependency>> = row.try_get("dependencies")?;
//         Ok(Self {
//             oid,
//             name: SchemaQualifiedName {
//                 local_name: name,
//                 schema_name: "".to_string(),
//             },
//             version,
//             schema_name,
//             is_relocatable,
//             dependencies: dependencies.0,
//         })
//     }
// }

impl SqlObject for Extension {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "EXTENSION"
    }

    fn dependency_declaration(&self) -> Dependency {
        Dependency {
            oid: Oid(0),
            catalog: PgCatalog::Extension,
        }
    }

    fn dependencies(&self) -> &[Dependency] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE EXTENSION {} VERSION {}", self.name, self.version)?;
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
