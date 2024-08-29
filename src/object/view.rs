use std::fmt::Write;

use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::{write_join, PgDiffError};

use super::{
    compare_option_lists, Dependency, OptionListObject, PgCatalog, SchemaQualifiedName, SqlObject,
};

pub async fn get_views(pool: &PgPool, schemas: &[&str]) -> Result<Vec<View>, PgDiffError> {
    let views_query = include_str!("./../../queries/views.pgsql");
    let views = match query_as(views_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load views");
            return Err(error.into());
        }
    };
    Ok(views)
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct View {
    pub(crate) oid: Oid,
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    pub(crate) columns: Option<Vec<String>>,
    pub(crate) query: String,
    pub(crate) options: Option<Vec<String>>,
    #[sqlx(json)]
    pub(crate) dependencies: Vec<Dependency>,
}

impl OptionListObject for View {}

impl SqlObject for View {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "VIEW"
    }

    fn dependency_declaration(&self) -> Dependency {
        Dependency {
            oid: self.oid,
            catalog: PgCatalog::Class,
        }
    }

    fn dependencies(&self) -> &[Dependency] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE OR REPLACE VIEW {}", self.name)?;
        if let Some(columns) = &self.columns {
            w.write_str("(")?;
            write_join!(w, columns.iter(), ",");
            w.write_str(")")?;
        }
        if let Some(options) = &self.options {
            w.write_str("(")?;
            write_join!(w, options.iter(), ",");
            w.write_str(")")?;
        }
        writeln!(w, " AS\n{}", self.query)?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.columns != new.columns {
            self.drop_statements(w)?;
            self.create_statements(w)?;
            return Ok(());
        }
        if self.query != new.query {
            self.create_statements(w)?;
        }
        compare_option_lists(self, self.options.as_deref(), new.options.as_deref(), w)?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP VIEW {};", self.name)?;
        Ok(())
    }
}
