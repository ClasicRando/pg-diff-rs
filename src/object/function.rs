use std::fmt::Write;

use serde::Deserialize;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool, query_as, Row};

use super::{SchemaQualifiedName, SqlObject};
use crate::PgDiffError;

pub async fn get_functions(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Function>, PgDiffError> {
    let functions_query = include_str!("../../queries/functions.pgsql");
    let functions = match query_as(functions_query)
        .bind(schemas)
        .fetch_all(pool)
        .await
    {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load functions");
            return Err(error.into());
        }
    };
    Ok(functions)
}

#[derive(Debug, PartialEq)]
pub struct Function {
    pub(crate) name: SchemaQualifiedName,
    pub(crate) is_procedure: bool,
    pub(crate) signature: String,
    pub(crate) definition: String,
    pub(crate) language: String,
    pub(crate) function_dependencies: Option<Vec<FunctionDependency>>,
}

impl<'r> FromRow<'r, PgRow> for Function {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let is_procedure: bool = row.try_get("is_procedure")?;
        let signature: String = row.try_get("signature")?;
        let definition: String = row.try_get("definition")?;
        let language: String = row.try_get("language")?;
        let function_dependencies: Option<Json<Vec<FunctionDependency>>> =
            row.try_get("function_dependencies")?;
        Ok(Self {
            name: name.0,
            is_procedure,
            signature,
            definition,
            language,
            function_dependencies: function_dependencies.map(|j| j.0),
        })
    }
}

impl SqlObject for Function {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        if self.is_procedure {
            "PROCEDURE"
        } else {
            "FUNCTION"
        }
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "{};", self.definition)?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.signature != new.signature {
            self.drop_statements(w)?;
        }
        self.create_statements(w)?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP {} {};", self.object_type_name(), self.name)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct FunctionDependency {
    pub(crate) name: SchemaQualifiedName,
    pub(crate) signature: String,
}
