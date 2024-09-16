use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::{query_as, PgPool};

use crate::{write_join, PgDiffError};

use super::{Collation, SchemaQualifiedName, SqlObject};

/// Fetch all UDT types found within the specified schemas. This includes composites, enums and
/// range types.
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

/// Struct representing a Postgres UDT. This encapsulates all UDT types supported by this
/// application.
#[derive(Debug, sqlx::FromRow)]
pub struct Udt {
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    #[sqlx(json)]
    pub(crate) udt_type: UdtType,
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for Udt {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.udt_type == other.udt_type
    }
}

impl SqlObject for Udt {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        self.udt_type.as_ref()
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
            UdtType::Domain {
                data_type,
                collation,
                default,
                is_not_null,
                checks,
            } => {
                write!(w, "CREATE DOMAIN {} AS {}", self.name, data_type)?;
                if let Some(collation) = collation {
                    write!(w, "\n    {}", collation)?;
                }
                if let Some(default) = default {
                    write!(w, "\n    DEFAULT {}", default)?;
                }
                write!(w, "\n    {}NULL", if *is_not_null { "NOT " } else { "" })?;
                match checks {
                    Some(checks) if !checks.is_empty() => {
                        write_join!(w, "\n    ", checks, "\n    ", "");
                    }
                    _ => {}
                }
                w.write_char(';')?;
            }
            _ => {
                return Err(PgDiffError::UnsupportedUdtType {
                    object_name: self.name.clone(),
                    type_name: self.udt_type.as_ref().to_string(),
                });
            }
        }
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if !self.udt_type.is_supported() {
            return Err(PgDiffError::UnsupportedUdtType {
                object_name: self.name.clone(),
                type_name: self.udt_type.as_ref().to_string(),
            });
        }
        if self.udt_type == new.udt_type {
            return Ok(());
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
            }
            (
                UdtType::Domain {
                    data_type: old_data_type,
                    collation: old_collation,
                    default: old_default,
                    is_not_null: old_is_not_null,
                    checks: old_checks,
                },
                UdtType::Domain {
                    data_type: new_data_type,
                    collation: new_collation,
                    default: new_default,
                    is_not_null: new_is_not_null,
                    checks: new_checks,
                },
            ) => {
                if old_data_type != new_data_type {
                    return Err(PgDiffError::InvalidMigration {
                        object_name: self.name.to_string(),
                        reason: format!(
                            "Cannot update domain type with new subtype. Existing subtype = '{}', New subtype = '{}'",
                            old_data_type,
                            new_data_type
                        ),
                    });
                }
                if old_collation != new_collation {
                    return Err(PgDiffError::InvalidMigration {
                        object_name: self.name.to_string(),
                        reason: format!(
                            "Cannot update domain collation. Existing collation = '{:?}', New collation = '{:?}'",
                            old_collation,
                            new_collation
                        ),
                    });
                }
                if old_default != new_default {
                    if old_default.is_some() {
                        writeln!(w, "ALTER DOMAIN {} DROP DEFAULT;", self.name)?;
                    }
                    if let Some(new) = new_default {
                        writeln!(w, "ALTER DOMAIN {} SET DEFAULT {};", self.name, new)?;
                    }
                }
                if old_is_not_null != new_is_not_null {
                    if *old_is_not_null {
                        writeln!(w, "ALTER DOMAIN {} DROP NOT NULL;", self.name)?;
                    }
                    if *new_is_not_null {
                        writeln!(w, "ALTER DOMAIN {} SET NOT NULL;", self.name)?;
                    }
                }
                if old_checks != new_checks {
                    for (old, new) in old_checks
                        .iter()
                        .flat_map(|o| o.iter())
                        .map(|o| {
                            (
                                o,
                                new_checks
                                    .iter()
                                    .flat_map(|n| n.iter())
                                    .find(|n| n.name == o.name),
                            )
                        })
                        .filter(|(o, n)| n.map(|n| n.expression == o.expression).unwrap_or(true))
                    {
                        writeln!(
                            w,
                            "ALTER DOMAIN {} DROP CONSTRAINT {};",
                            self.name, old.name
                        )?;
                        if let Some(new) = new {
                            writeln!(
                                w,
                                "ALTER DOMAIN {} ADD CONSTRAINT {} CHECK({});",
                                self.name, new.name, new.expression
                            )?;
                        }
                    }
                    for new in new_checks.iter().flat_map(|n| n.iter()).filter(|n| {
                        !old_checks
                            .iter()
                            .flat_map(|o| o.iter())
                            .any(|o| o.name == n.name)
                    }) {
                        writeln!(
                            w,
                            "ALTER DOMAIN {} ADD CONSTRAINT {} CHECK({});",
                            self.name, new.name, new.expression
                        )?;
                    }
                }
            }
            (_, _) => {
                return Err(PgDiffError::IncompatibleTypes {
                    name: self.name.clone(),
                    original_type: self.udt_type.as_ref().into(),
                    new_type: new.udt_type.as_ref().into(),
                })
            }
        }
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP TYPE {};", self.name)?;
        Ok(())
    }
}

/// UDT type variants
#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
#[serde(tag = "type")]
pub enum UdtType {
    /// Enum type containing 1 or more labels to constraint the field value
    #[strum(serialize = "enum")]
    Enum { labels: Vec<String> },
    /// Composite type containing 1 or more fields like linking to a sub table
    #[strum(serialize = "composite")]
    Composite { attributes: Vec<CompositeField> },
    /// Range type containing a subtype that this type ranges over
    #[strum(serialize = "range")]
    Range { subtype: String },
    /// Domain type containing a subtype
    #[strum(serialize = "domain")]
    Domain {
        data_type: String,
        collation: Option<Collation>,
        default: Option<String>,
        is_not_null: bool,
        checks: Option<Vec<DomainCheckConstraint>>,
    },
    #[strum(serialize = "base")]
    Base,
    #[strum(serialize = "pseudo")]
    Pseudo,
    #[strum(serialize = "multirange")]
    Multirange,
}

impl UdtType {
    /// Returns true if the migration of the type is supported
    pub fn is_supported(&self) -> bool {
        matches!(
            self,
            Self::Enum { .. } | Self::Composite { .. } | Self::Range { .. } | Self::Domain { .. }
        )
    }
}

/// UDT Composite fields metadata
#[derive(Debug, Deserialize, PartialEq)]
pub struct CompositeField {
    /// Field name
    pub(crate) name: String,
    /// Field data type
    pub(crate) data_type: String,
    /// Field data type size. Variable sized data is always -1.
    pub(crate) size: i32,
    /// Field data type collation if text based data.
    pub(crate) collation: Option<Collation>,
    /// True if this field is a base type. False means it's another UDT.
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

/// Container for domain check constraint details
#[derive(Debug, Deserialize, PartialEq)]
pub struct DomainCheckConstraint {
    name: String,
    expression: String,
}

impl Display for DomainCheckConstraint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CONSTRAINT {} {}", self.name, self.expression)
    }
}
