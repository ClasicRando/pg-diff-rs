use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::{query_as, PgPool};

use crate::{write_join, PgDiffError};

use super::{Collation, SchemaQualifiedName, SqlObject};

pub async fn get_udts(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Udt>, PgDiffError> {
    let udts_query = include_str!("./../../queries/udts.pgsql");
    let udts = match query_as(udts_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load udts");
            return Err(error.into());
        }
    };
    Ok(udts)
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Udt {
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    #[sqlx(json)]
    pub(crate) udt_type: UdtType,
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl SqlObject for Udt {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        self.udt_type.name()
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        match &self.udt_type {
            UdtType::Enum { labels } => {
                write!(w, "CREATE TYPE {} AS ENUM (\n    '", self.name)?;
                write_join!(w, labels, "',\n    '");
                w.write_str("'\n);\n")?;
            }
            UdtType::Composite { attributes } => {
                write!(w, "CREATE TYPE {} AS (\n    ", self.name)?;
                write_join!(w, attributes, ",\n    ");
                w.write_str("\n);\n")?;
            }
            UdtType::Range { subtype } => {
                writeln!(
                    w,
                    "CREATE TYPE {} AS RANGE (SUBTYPE = {});",
                    self.name, subtype
                )?;
            }
        }
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.udt_type != new.udt_type {
            return Err(PgDiffError::IncompatibleTypes {
                name: self.name.clone(),
                original_type: self.udt_type.name().into(),
                new_type: new.udt_type.name().into(),
            });
        }
        match (&self.udt_type, &new.udt_type) {
            (
                UdtType::Enum {
                    labels: existing_labels,
                },
                UdtType::Enum { labels: new_labels },
            ) => {
                let missing_labels: Vec<&String> = existing_labels
                    .iter()
                    .filter(|label| !new_labels.contains(*label))
                    .collect();
                if !missing_labels.is_empty() {
                    return Err(PgDiffError::InvalidMigration {
                        object_name: self.name.to_string(),
                        reason: format!(
                            "Enum has values removed during migration. Missing values: '{:?}'",
                            missing_labels
                        ),
                    });
                }

                for new_label in new_labels
                    .iter()
                    .filter(|label| !existing_labels.contains(*label))
                {
                    writeln!(w, "ALTER TYPE {} ADD VALUE '{new_label}';", self.name)?;
                }
                w.write_char('\n')?;
                Ok(())
            }
            (
                UdtType::Composite {
                    attributes: existing_attributes,
                },
                UdtType::Composite {
                    attributes: new_attributes,
                },
            ) => {
                let missing_attributes: Vec<&CompositeField> = existing_attributes
                    .iter()
                    .filter(|attribute| !new_attributes.iter().any(|a| attribute.name == a.name))
                    .collect();
                if !missing_attributes.is_empty() {
                    return Err(PgDiffError::InvalidMigration {
                        object_name: self.name.to_string(),
                        reason: format!(
                            "Composite has attributes removed during migration. Missing attributes: '{:?}'",
                            missing_attributes
                        ),
                    });
                }

                for attribute in new_attributes.iter().filter(|attribute| {
                    !existing_attributes.iter().any(|a| attribute.name == a.name)
                }) {
                    write!(
                        w,
                        "ALTER TYPE {} ADD ATTRIBUTE {} {}",
                        self.name, attribute.name, attribute.data_type,
                    )?;
                    if let Some(collation) = &attribute.collation {
                        write!(w, " COLLATE {collation}")?;
                    }
                    w.write_str(";\n")?;
                }
                Ok(())
            }
            (
                UdtType::Range {
                    subtype: existing_subtype,
                },
                UdtType::Range {
                    subtype: new_subtype,
                },
            ) => {
                if existing_subtype == new_subtype {
                    return Err(PgDiffError::InvalidMigration {
                        object_name: self.name.to_string(),
                        reason: format!(
                            "Cannot update range type with new subtype. Existing subtype = '{}', New subtype = '{}'",
                            existing_subtype,
                            new_subtype
                        ),
                    });
                }
                Ok(())
            }
            (_, _) => Err(PgDiffError::IncompatibleTypes {
                name: self.name.clone(),
                original_type: self.udt_type.name().into(),
                new_type: new.udt_type.name().into(),
            }),
        }
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP TYPE {};", self.name)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum UdtType {
    Enum { labels: Vec<String> },
    Composite { attributes: Vec<CompositeField> },
    Range { subtype: String },
}

impl UdtType {
    pub fn name(&self) -> &'static str {
        match self {
            UdtType::Enum { .. } => "enum",
            UdtType::Composite { .. } => "composite",
            UdtType::Range { .. } => "range",
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct CompositeField {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) size: i32,
    pub(crate) collation: Option<Collation>,
    pub(crate) is_base_type: bool,
}

impl Display for CompositeField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        match &self.collation {
            Some(collation) if !collation.is_default() => {
                write!(f, " {collation}")?;
            }
            _ => {}
        }
        Ok(())
    }
}
