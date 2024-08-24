use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::{PgPool, query_as};
use sqlx::postgres::types::Oid;

use crate::{join_display_iter, join_slice, PgDiffError};

use super::{SchemaQualifiedName, SqlObject};

pub async fn get_triggers(pool: &PgPool, tables: &[Oid]) -> Result<Vec<Trigger>, PgDiffError> {
    let triggers_query = include_str!("./../../queries/triggers.pgsql");
    let triggers = match query_as(triggers_query).bind(tables).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load triggers");
            return Err(error.into());
        }
    };
    Ok(triggers)
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Trigger {
    pub(crate) table_oid: Oid,
    pub(crate) name: String,
    #[sqlx(json)]
    pub(crate) schema_qualified_name: SchemaQualifiedName,
    #[sqlx(json)]
    pub(crate) owner_table_name: SchemaQualifiedName,
    pub(crate) timing: TriggerTiming,
    #[sqlx(json)]
    pub(crate) events: Vec<TriggerEvent>,
    pub(crate) old_name: Option<String>,
    pub(crate) new_name: Option<String>,
    pub(crate) is_row_level: bool,
    pub(crate) when_expression: Option<String>,
    #[sqlx(json)]
    pub(crate) function_name: SchemaQualifiedName,
    pub(crate) function_args: Option<Vec<u8>>,
}

impl SqlObject for Trigger {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "TRIGGER"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE TRIGGER {} {} ", self.name, self.timing.as_ref())?;
        join_display_iter(self.events.iter(), " ", w)?;
        write!(w, "\nON {}", self.owner_table_name)?;
        if self.old_name.is_some() || self.old_name.is_some() {
            write!(w, "\nREFERENCING")?;
        }
        if let Some(old_table) = &self.old_name {
            write!(w, " OLD TABLE AS {old_table}")?;
        }
        if let Some(new_table) = &self.new_name {
            write!(w, " NEW TABLE AS {new_table}")?;
        }
        write!(
            w,
            "\nFOR EACH {}",
            if self.is_row_level {
                "ROW"
            } else {
                "STATEMENT"
            }
        )?;
        if let Some(when_expression) = &self.when_expression {
            write!(w, "\nWHEN {when_expression}")?;
        }
        write!(w, "\nEXECUTE FUNCTION {}(", self.function_name)?;
        match &self.function_args {
            Some(args) if !args.is_empty() => {
                w.write_char('\'')?;
                let iter = args.split(|byte| *byte == 0).filter_map(|chunk| {
                    let str = String::from_utf8_lossy(chunk);
                    if str.is_empty() {
                        return None;
                    }
                    Some(str)
                });
                join_display_iter(iter, "','", w)?;
                w.write_char('\'')?;
            }
            _ => {}
        }
        writeln!(w, ");")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, _: &Self, w: &mut W) -> Result<(), PgDiffError> {
        self.drop_statements(w)?;
        self.create_statements(w)
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(
            w,
            "DROP TRIGGER {} ON {};",
            self.name, self.owner_table_name
        )?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, strum::AsRefStr, sqlx::Type)]
#[sqlx(type_name = "text")]
pub enum TriggerTiming {
    #[sqlx(rename = "before")]
    #[strum(serialize = "BEFORE")]
    Before,
    #[sqlx(rename = "after")]
    #[strum(serialize = "AFTER")]
    After,
    #[sqlx(rename = "instead-of")]
    #[strum(serialize = "INSTEAD OF")]
    InsteadOf,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum TriggerEvent {
    #[serde(rename = "insert")]
    Insert,
    #[serde(rename = "update")]
    Update { columns: Option<Vec<String>> },
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "truncate")]
    Truncate,
}

impl Display for TriggerEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerEvent::Insert => write!(f, "INSERT"),
            TriggerEvent::Update { columns } => {
                write!(f, "UPDATE")?;
                if let Some(columns) = columns {
                    write!(f, " OF ")?;
                    join_slice(columns.as_slice(), ",", f)?;
                }
                Ok(())
            }
            TriggerEvent::Delete => write!(f, "DELETE"),
            TriggerEvent::Truncate => write!(f, "TRUNCATE"),
        }
    }
}
