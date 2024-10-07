use std::fmt::{Display, Formatter, Write};

use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::{impl_type_for_kvp_wrapper, write_join, PgDiffError};

use super::{compare_key_value_pairs, KeyValuePairs, SchemaQualifiedName, SqlObject};

/// Fetch all views found within the specified schemas
pub async fn get_views(pool: &PgPool, schemas: &[&str]) -> Result<Vec<View>, PgDiffError> {
    let views_query = include_str!("./../../queries/views.pgsql");
    let views = match query_as(views_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load views");
            return Err(error.into());
        },
    };
    Ok(views)
}

#[derive(Debug, PartialEq)]
pub struct ViewOptions(KeyValuePairs);

impl_type_for_kvp_wrapper!(ViewOptions);

impl Display for ViewOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        write_join!(
            f,
            "WITH(",
            self.0.iter(),
            |w, (key, value)| write!(w, "{key}={value}"),
            ",",
            ")"
        );
        Ok(())
    }
}

/// Struct representing a SQL view
#[derive(Debug, sqlx::FromRow)]
pub struct View {
    /// View OID
    pub(crate) oid: Oid,
    /// Full name of the view
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    /// Columns specified for the view
    pub(crate) columns: Option<Vec<String>>,
    /// Query representing the view result
    pub(crate) query: String,
    /// View options supplied. All items are key value pairs separated by `=`
    pub(crate) options: Option<ViewOptions>,
    /// Dependencies of the view
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for View {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.columns == other.columns
            && self.query == other.query
            && self.options == other.options
    }
}

impl SqlObject for View {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "VIEW"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE OR REPLACE VIEW {}", self.name)?;
        if let Some(columns) = &self.columns {
            write_join!(w, "(", columns, ",", ")");
        }
        if let Some(options) = &self.options {
            write!(w, "{options}")?;
        }
        writeln!(w, " AS\n{}", self.query)?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.query != new.query || self.columns != new.columns {
            self.drop_statements(w)?;
            self.create_statements(w)?;
            return Ok(());
        }
        compare_key_value_pairs(w, self, &self.options, &new.options, false)?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP VIEW {};", self.name)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {}
