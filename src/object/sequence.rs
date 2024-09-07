use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{query_as, FromRow, PgPool, Row};

use crate::PgDiffError;

use super::{SchemaQualifiedName, SqlObject};

pub async fn get_sequences(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Sequence>, PgDiffError> {
    let sequence_query = include_str!("./../../queries/sequences.pgsql");
    let sequences = match query_as(sequence_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load sequences");
            return Err(error.into());
        }
    };
    Ok(sequences)
}

#[derive(Debug, PartialEq)]
pub struct Sequence {
    pub(crate) name: SchemaQualifiedName,
    pub(crate) data_type: String,
    pub(crate) owner: Option<SequenceOwner>,
    pub(crate) sequence_options: SequenceOptions,
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl<'r> FromRow<'r, PgRow> for Sequence {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let data_type = row.try_get("data_type")?;
        let owner: Option<Json<SequenceOwner>> = row.try_get("owner")?;
        let sequence_options: SequenceOptions = SequenceOptions {
            increment: row.try_get("increment")?,
            min_value: row.try_get("min_value")?,
            max_value: row.try_get("max_value")?,
            start_value: row.try_get("start_value")?,
            cache: row.try_get("cache")?,
            is_cycle: row.try_get("is_cycle")?,
        };
        let dependencies: Json<Vec<SchemaQualifiedName>> = row.try_get("dependencies")?;
        Ok(Self {
            name: name.0,
            data_type,
            owner: owner.map(|j| j.0),
            sequence_options,
            dependencies: dependencies.0,
        })
    }
}

impl SqlObject for Sequence {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "SEQUENCE"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(
            w,
            "CREATE SEQUENCE {} AS {} {}",
            self.name, self.data_type, self.sequence_options,
        )?;
        if let Some(owner) = &self.owner {
            writeln!(w, " {owner};")?;
        } else {
            w.write_str(" OWNED BY NONE;\n")?;
        }
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "ALTER SEQUENCE {}", self.name)?;
        if self.data_type != new.data_type {
            write!(w, " AS {}", new.data_type)?;
        }
        if self.sequence_options.increment != new.sequence_options.increment {
            write!(w, " INCREMENT {}", new.sequence_options.increment)?;
        }
        if self.sequence_options.min_value != new.sequence_options.min_value {
            write!(w, " MINVALUE {}", new.sequence_options.min_value)?;
        }
        if self.sequence_options.max_value != new.sequence_options.max_value {
            write!(w, " MAXVALUE {}", new.sequence_options.max_value)?;
        }
        if self.sequence_options.start_value != new.sequence_options.start_value {
            write!(w, " START WITH {}", new.sequence_options.start_value)?;
        }
        if self.sequence_options.cache != new.sequence_options.cache {
            write!(w, " CACHE {}", new.sequence_options.cache)?;
        }
        if self.sequence_options.is_cycle != new.sequence_options.is_cycle {
            write!(
                w,
                " {}CYCLE",
                if new.sequence_options.is_cycle {
                    ""
                } else {
                    "NO "
                }
            )?;
        }
        match (&self.owner, &new.owner) {
            (Some(old_owner), Some(new_owner)) if old_owner != new_owner => {
                write!(w, " OWNED BY {new_owner}")?;
            }
            (Some(_), None) => {
                w.write_str(" OWNED BY NONE")?;
            }
            (None, Some(new_owner)) => {
                write!(w, " OWNED BY {new_owner}")?;
            }
            _ => {}
        }
        w.write_str(";\n")?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP SEQUENCE {};", self.name)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct SequenceOptions {
    pub(crate) increment: i64,
    pub(crate) min_value: i64,
    pub(crate) max_value: i64,
    pub(crate) start_value: i64,
    pub(crate) cache: i64,
    pub(crate) is_cycle: bool,
}

impl Display for SequenceOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "INCREMENT {} MINVALUE {} MAXVALUE {} START {} CACHE {} {} CYCLE",
            self.increment,
            self.min_value,
            self.max_value,
            self.start_value,
            self.cache,
            if self.is_cycle { "" } else { "NO" }
        )
    }
}

impl SequenceOptions {
    pub fn alter_sequence<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(
            w,
            "SET INCREMENT {} SET MINVALUE {} SET MAXVALUE {} SET START {} SET CACHE {} SET {} CYCLE",
            self.increment,
            self.min_value,
            self.max_value,
            self.start_value,
            self.cache,
            if self.is_cycle { "" } else { "NO" }
        )?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct SequenceOwner {
    pub(crate) table_name: SchemaQualifiedName,
    pub(crate) column_name: String,
}

impl Display for SequenceOwner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OWNED BY {}.{}", self.table_name, self.column_name)
    }
}
