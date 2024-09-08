use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::object::{IndexParameters, SchemaQualifiedName, SqlObject};
use crate::{write_join, PgDiffError};

pub async fn get_constraints(
    pool: &PgPool,
    tables: &[Oid],
) -> Result<Vec<Constraint>, PgDiffError> {
    let constraints_query = include_str!("./../../queries/constraints.pgsql");
    let constraints = match query_as(constraints_query)
        .bind(tables)
        .fetch_all(pool)
        .await
    {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load constraints");
            return Err(error.into());
        }
    };
    Ok(constraints)
}

#[derive(Debug, Deserialize, sqlx::FromRow)]
pub struct Constraint {
    pub(crate) table_oid: Oid,
    #[sqlx(json)]
    pub(crate) owner_table_name: SchemaQualifiedName,
    pub(crate) name: String,
    #[sqlx(json)]
    pub(crate) schema_qualified_name: SchemaQualifiedName,
    #[sqlx(json)]
    pub(crate) constraint_type: ConstraintType,
    #[sqlx(json)]
    pub(crate) timing: ConstraintTiming,
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for Constraint {
    #[inline]
    fn eq(&self, other: &Constraint) -> bool {
        self.owner_table_name == other.owner_table_name
            && self.name == other.name
            && self.schema_qualified_name == other.schema_qualified_name
            && self.constraint_type == other.constraint_type
            && self.timing == other.timing && self.dependencies == other.dependencies
    }
}

impl SqlObject for Constraint {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "CONSTRAINT"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        match &self.constraint_type {
            ConstraintType::Check {
                expression,
                is_inheritable,
                ..
            } => write!(
                w,
                "ALTER TABLE {} ADD CONSTRAINT {}\n{}{}",
                self.owner_table_name,
                self.name,
                expression.trim(),
                if *is_inheritable { "" } else { " NO INHERIT" }
            )?,
            ConstraintType::Unique {
                columns,
                are_nulls_distinct,
                index_parameters,
            } => {
                write!(
                    w,
                    "ALTER TABLE {} ADD CONSTRAINT {}\nUNIQUE NULLS{} DISTINCT (",
                    self.owner_table_name,
                    self.name,
                    if *are_nulls_distinct { "" } else { " NOT" },
                )?;
                write_join!(w, columns, ",");
                write!(w, "){index_parameters}")?;
            }
            ConstraintType::PrimaryKey {
                columns,
                index_parameters,
            } => {
                write!(
                    w,
                    "ALTER TABLE {} ADD CONSTRAINT {}\nPRIMARY KEY (",
                    self.owner_table_name, self.name,
                )?;
                write_join!(w, columns, ",");
                write!(w, "){index_parameters}")?;
            }
            ConstraintType::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                match_type,
                on_delete,
                on_update,
            } => {
                write!(
                    w,
                    "ALTER TABLE {} ADD CONSTRAINT {}\nFOREIGN KEY (",
                    self.owner_table_name, self.name,
                )?;
                write_join!(w, columns, ",");
                write!(w, ") REFERENCES {ref_table}(")?;
                write_join!(w, ref_columns, ",");
                write!(
                    w,
                    ") {}\n    ON DELETE {on_delete}\n    ON UPDATE {on_update}",
                    match_type.as_ref(),
                )?;
            }
        };
        writeln!(w, " {};", self.timing)?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.constraint_type == new.constraint_type && self.timing == new.timing {
            return Ok(());
        }

        if self.constraint_type != new.constraint_type {
            writeln!(
                w,
                "ALTER TABLE {} DROP CONSTRAINT {};",
                self.owner_table_name, self.name
            )?;
            self.create_statements(w)?;
            return Ok(());
        }

        if self.timing != new.timing {
            writeln!(
                w,
                "ALTER TABLE {} ALTER CONSTRAINT {} {};",
                self.owner_table_name, self.name, new.timing
            )?;
        }

        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(
            w,
            "ALTER TABLE {} DROP CONSTRAINT {};",
            self.owner_table_name, self.name
        )?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ConstraintType {
    Check {
        columns: Vec<String>,
        expression: String,
        is_inheritable: bool,
    },
    Unique {
        columns: Vec<String>,
        are_nulls_distinct: bool,
        index_parameters: IndexParameters,
    },
    PrimaryKey {
        columns: Vec<String>,
        index_parameters: IndexParameters,
    },
    ForeignKey {
        columns: Vec<String>,
        ref_table: SchemaQualifiedName,
        ref_columns: Vec<String>,
        match_type: ForeignKeyMatch,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ConstraintTiming {
    #[default]
    NotDeferrable,
    Deferrable {
        is_immediate: bool,
    },
}

impl Display for ConstraintTiming {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            ConstraintTiming::NotDeferrable => "NOT DEFERRABLE",
            ConstraintTiming::Deferrable { is_immediate } => {
                if *is_immediate {
                    "DEFERRABLE INITIALLY IMMEDIATE"
                } else {
                    "DEFERRABLE INITIALLY DEFERRED"
                }
            }
        };
        f.write_str(text)
    }
}

#[derive(Debug, Default, Deserialize, PartialEq, strum::AsRefStr)]
pub enum ForeignKeyMatch {
    #[strum(serialize = "MATCH FULL")]
    Full,
    #[strum(serialize = "MATCH PARTIAL")]
    Partial,
    #[default]
    #[strum(serialize = "MATCH SIMPLE")]
    Simple,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ForeignKeyAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull {
        columns: Option<Vec<String>>,
    },
    SetDefault {
        columns: Option<Vec<String>>,
    },
}

impl Display for ForeignKeyAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ForeignKeyAction::NoAction => write!(f, "NO ACTION"),
            ForeignKeyAction::Restrict => write!(f, "RESTRICT"),
            ForeignKeyAction::Cascade => write!(f, "CASCADE"),
            ForeignKeyAction::SetNull { columns } => {
                if let Some(columns) = columns {
                    write!(f, "SET NULL (")?;
                    write_join!(f, columns, ",");
                    write!(f, ")")
                } else {
                    write!(f, "SET NULL")
                }
            }
            ForeignKeyAction::SetDefault { columns } => {
                if let Some(columns) = columns {
                    write!(f, "SET DEFAULT (")?;
                    write_join!(f, columns, ",");
                    write!(f, ")")
                } else {
                    write!(f, "SET DEFAULT")
                }
            }
        }
    }
}
