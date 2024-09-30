use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::{write_join, PgDiffError};

use super::{SchemaQualifiedName, SqlObject};

/// Fetch all triggers associated with the objects referenced (by OID)
pub async fn get_triggers(pool: &PgPool, object_oids: &[Oid]) -> Result<Vec<Trigger>, PgDiffError> {
    let triggers_query = include_str!("./../../queries/triggers.pgsql");
    let triggers = match query_as(triggers_query)
        .bind(object_oids)
        .fetch_all(pool)
        .await
    {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load triggers");
            return Err(error.into());
        },
    };
    Ok(triggers)
}

/// Struct representing a SQL trigger object
#[derive(Debug, sqlx::FromRow)]
pub struct Trigger {
    /// Owner object OID
    pub(crate) owner_oid: Oid,
    /// Name of the trigger
    pub(crate) name: String,
    /// Full name of the trigger with the table name as a prefix
    #[sqlx(json)]
    pub(crate) schema_qualified_name: SchemaQualifiedName,
    /// Full name of the owner object (table/view)
    #[sqlx(json)]
    pub(crate) owner_object_name: SchemaQualifiedName,
    /// Trigger timing option
    pub(crate) timing: TriggerTiming,
    /// 1 or more events that drive the trigger
    #[sqlx(json)]
    pub(crate) events: Vec<TriggerEvent>,
    /// Name of the new transition table if the trigger is statement level. [None] if row level.
    pub(crate) old_name: Option<String>,
    /// Name of the old transition table if the trigger is statement level. [None] if row level.
    pub(crate) new_name: Option<String>,
    /// True if the trigger is row level. False means the trigger is statement level.
    pub(crate) is_row_level: bool,
    /// Option when expression to only run the trigger if the expression is true
    pub(crate) when_expression: Option<String>,
    /// Full name of the trigger function executed
    #[sqlx(json)]
    pub(crate) function_name: SchemaQualifiedName,
    /// Optional function arguments supplied to the trigger function on each call. The data is
    /// stored in the database as `bytea` so it's present here as raw bytes. To access this
    /// information as text use [Trigger::write_function_arguments].
    pub(crate) function_args: Option<Vec<u8>>,
    /// Dependencies of the trigger. This is always the table and trigger function
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for Trigger {
    #[inline]
    fn eq(&self, other: &Trigger) -> bool {
        self.name == other.name
            && self.schema_qualified_name == other.schema_qualified_name
            && self.owner_object_name == other.owner_object_name
            && self.timing == other.timing
            && self.events == other.events
            && self.old_name == other.old_name
            && self.new_name == other.new_name
            && self.is_row_level == other.is_row_level
            && self.when_expression == other.when_expression
            && self.function_name == other.function_name
            && self.function_args == other.function_args
    }
}

impl Trigger {
    /// Extract the text of the arguments and write the string to the writeable object.
    ///
    /// The arguments are in a null byte separated UTF8 string so the text is extracted by splitting
    /// the byte array by 0 and each chunk is passed to [String::from_utf8_lossy] since there should
    /// be no actual loss due to the unicode errors.
    fn write_function_arguments<W>(&self, w: &mut W) -> Result<(), std::fmt::Error>
    where
        W: Write,
    {
        match &self.function_args {
            Some(args) if !args.is_empty() => {
                w.write_char('\'')?;
                write_join!(
                    w,
                    args.split(|byte| *byte == 0).filter_map(|chunk| {
                        let str = String::from_utf8_lossy(chunk);
                        if str.is_empty() {
                            return None;
                        }
                        Some(str)
                    }),
                    "','"
                );
                w.write_char('\'')?;
            },
            _ => {},
        }
        Ok(())
    }
}

impl SqlObject for Trigger {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "TRIGGER"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE TRIGGER {} {} ", self.name, self.timing.as_ref())?;
        write_join!(w, self.events.iter(), " OR ");
        write!(w, "\nON {}", self.owner_object_name)?;
        if self.old_name.is_some() || self.old_name.is_some() {
            w.write_str("\nREFERENCING")?;
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
        self.write_function_arguments(w)?;
        w.write_str(");\n")?;
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
            self.name, self.owner_object_name
        )?;
        Ok(())
    }
}

/// Trigger timing variants
#[derive(Debug, PartialEq, strum::AsRefStr, sqlx::Type)]
#[sqlx(type_name = "text")]
pub enum TriggerTiming {
    /// Trigger executes before the actual operation
    #[sqlx(rename = "before")]
    #[strum(serialize = "BEFORE")]
    Before,
    /// Trigger executes after the actual operation
    #[sqlx(rename = "after")]
    #[strum(serialize = "AFTER")]
    After,
    /// Trigger replaces the actual operation. Only valid for views.
    #[sqlx(rename = "instead-of")]
    #[strum(serialize = "INSTEAD OF")]
    InsteadOf,
}

/// Event that is tracked for the trigger
#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum TriggerEvent {
    /// Table/View `INSERT`
    #[serde(rename = "insert")]
    Insert,
    /// Table/View `UPDATE`. Optionally also includes specifying only certain fields to track
    #[serde(rename = "update")]
    Update { columns: Option<Vec<String>> },
    /// Table/View `DELETE`
    #[serde(rename = "delete")]
    Delete,
    /// Table/View `TRUNCATE`
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
                    write_join!(f, columns, ",");
                }
                Ok(())
            },
            TriggerEvent::Delete => write!(f, "DELETE"),
            TriggerEvent::Truncate => write!(f, "TRUNCATE"),
        }
    }
}
