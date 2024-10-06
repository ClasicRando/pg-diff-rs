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

#[cfg(test)]
mod test {
    use crate::object::SqlObject;

    use super::Extension;
    const NAME: &str = "test_extension";
    const VERSION_1: &str = "1.0";
    const VERSION_2: &str = "2.0";
    const SCHEMA_NAME: &str = "test_schema";
    const OTHER_SCHEMA_NAME: &str = "other_test_schema";

    fn create_extension(version: &str, schema_name: &str, is_relocatable: bool) -> Extension {
        Extension {
            name: NAME.into(),
            version: version.into(),
            schema_name: schema_name.into(),
            is_relocatable,
            dependencies: vec![],
        }
    }

    #[rstest::rstest]
    #[case(
        create_extension(
            VERSION_1,
            SCHEMA_NAME,
            true
        ),
        include_str!("../../test-files/sql/extension-create-case1.pgsql"),
    )]
    #[case(
        create_extension(
            VERSION_1,
            SCHEMA_NAME,
            false
        ),
        include_str!("../../test-files/sql/extension-create-case2.pgsql"),
    )]
    fn create_statements_should_add_create_extension_statement(
        #[case] extension: Extension,
        #[case] statement: &str,
    ) {
        let mut writeable = String::new();

        extension.create_statements(&mut writeable).unwrap();

        assert_eq!(statement.trim(), writeable.trim())
    }

    #[rstest::rstest]
    #[case(
        create_extension(
            VERSION_1,
            SCHEMA_NAME,
            true
        ),
        create_extension(
            VERSION_1,
            OTHER_SCHEMA_NAME,
            true
        ),
        include_str!("../../test-files/sql/extension-alter-case1.pgsql"),
    )]
    #[case(
        create_extension(
            VERSION_1,
            SCHEMA_NAME,
            false
        ),
        create_extension(
            VERSION_2,
            SCHEMA_NAME,
            false
        ),
        include_str!("../../test-files/sql/extension-alter-case2.pgsql"),
    )]
    fn alter_statements_should_add_alter_extension_statements(
        #[case] old: Extension,
        #[case] new: Extension,
        #[case] statement: &str,
    ) {
        let mut writeable = String::new();

        old.alter_statements(&new, &mut writeable).unwrap();

        assert_eq!(statement.trim(), writeable.trim())
    }

    #[test]
    fn drop_statements_should_add_drop_extension_statement() {
        let extension = create_extension(VERSION_1, SCHEMA_NAME, true);
        let statement = include_str!("../../test-files/sql/extension-drop.pgsql");
        let mut writeable = String::new();

        extension.drop_statements(&mut writeable).unwrap();

        assert_eq!(statement.trim(), writeable.trim());
    }
}
