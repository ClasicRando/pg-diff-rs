use std::fmt::Write;

use sqlx::postgres::PgRow;
use sqlx::{query_as, FromRow, PgPool, Row};

use crate::PgDiffError;

use super::{SchemaQualifiedName, SqlObject};

/// Fetch all schemas found within the current database (including the `public` schema).
///
/// Excludes `pg_catalog`, `information_schema` and all schemas named like `^pg_toast` and
/// `^pg_temp`. These schemas always exist but should not be analyzed.  
pub async fn get_schemas(pool: &PgPool) -> Result<Vec<Schema>, PgDiffError> {
    let schemas_query = include_str!("./../../queries/schemas.pgsql");
    let schema_names = match query_as(schemas_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load schemas");
            return Err(error.into());
        },
    };
    Ok(schema_names)
}

/// Struct representing a schema SQL object
#[derive(Debug, PartialEq)]
pub struct Schema {
    /// Name of the schema. Local part is always empty
    pub(crate) name: SchemaQualifiedName,
    /// Owner role of this schema
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

    fn dependencies(&self) -> &[SchemaQualifiedName] {
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

    fn dependencies_met(&self, _: &[&SchemaQualifiedName]) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use crate::object::{SchemaQualifiedName, SqlObject};
    use super::Schema;

    const SCHEMA_NAME: &str = "test_schema";
    
    const TEST_USER: &str = "test_user";
    
    const OTHER_TEST_USER: &str = "other_test_user";
    
    fn create_schema(owner: String) -> Schema {
        Schema {
            name: SchemaQualifiedName::new(SCHEMA_NAME, ""),
            owner,
        }
    }

    #[test]
    fn create_statements_should_add_create_schema_statement() {
        let schema = create_schema(TEST_USER.to_string());
        let statement = include_str!("../../test-files/sql/schema-create.pgsql");
        let mut writeable = String::new();

        schema.create_statements(&mut writeable).unwrap();

        assert_eq!(statement.trim(), writeable.trim());
    }


    #[test]
    fn alter_statements_should_add_alter_schema_statement() {
        let old = create_schema(TEST_USER.to_string());
        let new = create_schema(OTHER_TEST_USER.to_string());
        let statement = include_str!("../../test-files/sql/schema-alter.pgsql");
        let mut writeable = String::new();

        old.alter_statements(&new, &mut writeable).unwrap();

        assert_eq!(statement.trim(), writeable.trim());
    }

    #[test]
    fn drop_statements_should_add_drop_schema_statement() {
        let schema = create_schema(TEST_USER.to_string());
        let statement = include_str!("../../test-files/sql/schema-drop.pgsql");
        let mut writeable = String::new();

        schema.drop_statements(&mut writeable).unwrap();

        assert_eq!(statement.trim(), writeable.trim());
    }
}
