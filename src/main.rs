use std::convert::Into;
use std::fmt::{Display, Formatter, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{Parser, Subcommand};
use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{PgConnectOptions, PgRow};
use sqlx::types::Json;
use sqlx::{query_as, FromRow, PgPool, Row, Error};
use thiserror::Error as ThisError;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(Debug, ThisError)]
pub enum PgDiffError {
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
    #[error("{0}")]
    General(String),
    #[error("For {name}, found new type '{new_type}' that is incompatible with existing type {original_type}")]
    IncompatibleTypes {
        name: SchemaQualifiedName,
        original_type: String,
        new_type: String,
    },
    #[error("Could not construct a migration strategy for {object_name}. {reason}")]
    InvalidMigration { object_name: String, reason: String },
    #[error("This can never happen")]
    Infallible(#[from] std::convert::Infallible),
}

fn map_join_slice<I, F: Fn(&I, &mut W) -> Result<(), std::fmt::Error>, W: Write>(
    slice: &[I],
    map: F,
    separator: &str,
    w: &mut W,
) -> Result<(), std::fmt::Error> {
    let mut iter = slice.iter();
    let Some(item) = iter.next() else {
        return Ok(());
    };
    map(item, w)?;
    for item in iter {
        write!(w, "{separator}")?;
        map(item, w)?;
    }
    Ok(())
}

fn join_display_iter<D: Display, I: Iterator<Item = D>, W: Write>(
    mut iter: I,
    separator: &str,
    w: &mut W,
) -> Result<(), std::fmt::Error> {
    let Some(item) = iter.next() else {
        return Ok(());
    };
    write!(w, "{item}")?;
    for item in iter {
        write!(w, "{separator}")?;
        write!(w, "{item}")?;
    }
    Ok(())
}

fn join_slice<I: AsRef<str>, W: Write>(
    slice: &[I],
    separator: &str,
    w: &mut W,
) -> Result<(), std::fmt::Error> {
    let mut iter = slice.iter();
    let Some(item) = iter.next() else {
        return Ok(());
    };
    write!(w, "{}", item.as_ref())?;
    for item in iter {
        write!(w, "{separator}")?;
        write!(w, "{}", item.as_ref())?;
    }
    Ok(())
}

trait SqlObject: PartialEq {
    fn name(&self) -> &SchemaQualifiedName;
    fn object_type_name(&self) -> &str;
    /// Create the `CREATE` statement for this object
    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError>;
    /// Create the `ALTER` statement(s) required for this SQL object to be migrated to the new state
    /// provided.
    ///
    /// ## Errors
    /// If the migration is not possible either due to an unsupported, impossible or invalid
    /// migration.  
    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError>;
    /// Create the `DROP` statement for this object
    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError>;
}

#[derive(Debug)]
pub struct CustomType {
    oid: Oid,
    name: String,
}

#[derive(Debug)]
pub struct Database {
    schemas: Vec<Schema>,
    udts: Vec<Udt>,
    tables: Vec<Table>,
    constraints: Vec<Constraint>,
    indexes: Vec<Index>,
    triggers: Vec<Trigger>,
    sequences: Vec<Sequence>,
    functions: Vec<Function>,
    views: Vec<View>,
    extensions: Vec<Extension>,
}

#[derive(Debug, PartialEq)]
pub struct Schema {
    name: SchemaQualifiedName,
    owner: String,
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
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Deserialize)]
pub struct SchemaQualifiedName {
    schema_name: String,
    local_name: String,
}

impl Display for SchemaQualifiedName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.schema_name.is_empty() {
            return write!(f, "{}", self.local_name);
        }
        if self.local_name.is_empty() {
            return write!(f, "{}", self.schema_name);
        }
        write!(f, "{}.{}", self.schema_name, self.local_name)
    }
}

#[derive(Debug, PartialEq, Deserialize, sqlx::Type)]
#[sqlx(transparent)]
pub struct Collation(String);

impl Display for Collation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "COLLATE {}", self.0)
    }
}

impl Collation {
    fn is_default(&self) -> bool {
        self.0.as_str() == "\"pg_catalog\".\"default\""
    }
}

#[derive(Debug, PartialEq)]
pub struct Extension {
    name: SchemaQualifiedName,
    version: String,
    schema_name: String,
    is_relocatable: bool,
}

impl<'r> FromRow<'r, PgRow> for Extension {
    fn from_row(row: &'r PgRow) -> Result<Self, Error> {
        let name: String = row.try_get("name")?;
        let version: String = row.try_get("version")?;
        let schema_name: String = row.try_get("schema_name")?;
        let is_relocatable: bool = row.try_get("is_relocatable")?;
        Ok(Self {
            name: SchemaQualifiedName {
                local_name: name,
                schema_name: "".to_string()
            },
            version,
            schema_name,
            is_relocatable,
        })
    }
}

impl SqlObject for Extension {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "EXTENSION"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE EXTENSION {} VERSION {}", self.name, self.version)?;
        if self.is_relocatable {
            write!(w, " SCHEMA {}", self.schema_name)?;
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.schema_name != new.schema_name && self.is_relocatable {
            writeln!(w, "ALTER EXTENSION {} SET SCHEMA {};", self.name, new.schema_name)?;
        }
        if self.version != new.version {
            writeln!(w, "ALTER EXTENSION {} UPDATE TO {};", self.name, new.version)?;
        }
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP EXTENSION {};", self.name)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Udt {
    #[sqlx(json)]
    name: SchemaQualifiedName,
    #[sqlx(json)]
    udt_type: UdtType,
}

impl SqlObject for Udt {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        self.udt_type.name()
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        match &self.udt_type {
            UdtType::Enum { labels } => {
                write!(w, "CREATE TYPE {} AS ENUM (\n\t'", self.name)?;
                join_slice(labels.as_slice(), "',\n\t'", w)?;
                writeln!(w, "'\n);")?;
            }
            UdtType::Composite { attributes } => {
                write!(w, "CREATE TYPE {} AS (\n\t", self.name)?;
                join_display_iter(attributes.iter(), ",\n\t", w)?;
                writeln!(w, "\n);")?;
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
                    writeln!(w, ";")?;
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
    fn name(&self) -> &'static str {
        match self {
            UdtType::Enum { .. } => "enum",
            UdtType::Composite { .. } => "composite",
            UdtType::Range { .. } => "range",
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct CompositeField {
    name: String,
    data_type: String,
    size: i32,
    collation: Option<Collation>,
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

#[derive(Debug, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct TableSpace(String);

impl Display for TableSpace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct TablespaceCompare<'a> {
    old: Option<&'a TableSpace>,
    new: Option<&'a TableSpace>,
}

impl<'a> TablespaceCompare<'a> {
    fn has_diff(&self) -> bool {
        match (self.old, self.new) {
            (Some(old_tablespace), Some(new_tablespace)) => old_tablespace != new_tablespace,
            (Some(_), None) => true,
            (None, Some(_)) => true,
            _ => false,
        }
    }
}

impl<'a> Display for TablespaceCompare<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match (self.old, self.new) {
            (Some(old_tablespace), Some(new_tablespace)) if old_tablespace != new_tablespace => {
                write!(f, "SET TABLESPACE {new_tablespace}")
            }
            (Some(_), None) => {
                write!(f, "SET TABLESPACE pg_default")
            }
            (None, Some(new_tablespace)) => {
                write!(f, "SET TABLESPACE {new_tablespace}")
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Table {
    oid: Oid,
    name: SchemaQualifiedName,
    columns: Vec<Column>,
    policies: Option<Vec<Policy>>,
    partition_key_def: Option<String>,
    partition_values: Option<String>,
    inherited_tables: Option<Vec<SchemaQualifiedName>>,
    partitioned_parent_table: Option<SchemaQualifiedName>,
    tablespace: Option<TableSpace>,
    with: Option<Vec<StorageParameter>>,
}

impl<'r> FromRow<'r, PgRow> for Table {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let oid: Oid = row.try_get("oid")?;
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let columns: Json<Vec<Column>> = row.try_get("columns")?;
        let policies: Option<Json<Vec<Policy>>> = row.try_get("policies")?;
        let partition_key_def: Option<String> = row.try_get("partition_key_def")?;
        let partition_values: Option<String> = row.try_get("partition_values")?;
        let inherited_tables: Option<Json<Vec<SchemaQualifiedName>>> =
            row.try_get("inherited_tables")?;
        let partitioned_parent_table: Option<Json<SchemaQualifiedName>> =
            row.try_get("partitioned_parent_table")?;
        let tablespace: Option<TableSpace> = row.try_get("tablespace")?;
        let with: Option<Vec<StorageParameter>> = row.try_get("with")?;
        Ok(Self {
            oid,
            name: name.0,
            columns: columns.0,
            policies: policies.map(|j| j.0),
            partition_key_def,
            partition_values,
            inherited_tables: inherited_tables.map(|j| j.0),
            partitioned_parent_table: partitioned_parent_table.map(|j| j.0),
            tablespace,
            with,
        })
    }
}

impl SqlObject for Table {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "TABLE"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "CREATE TABLE {}", self.name)?;
        if let Some(partitioned_parent_table) = &self.partitioned_parent_table {
            write!(w, "PARTITION OF {partitioned_parent_table}")?;
        } else if !self.columns.is_empty() {
            write!(w, "(\n\t")?;
            map_join_slice(
                self.columns.as_slice(),
                |c, s| c.field_definition(true, s),
                ",\n\t",
                w,
            )?;
            write!(w, "\n)")?;
        }
        match &self.partition_values {
            Some(partition_values) => {
                write!(w, "\nFOR VALUES {partition_values}")?;
            }
            None if self.partitioned_parent_table.is_some() => {
                write!(w, "\nDEFAULT")?;
            }
            _ => {}
        }
        match &self.inherited_tables {
            Some(inherited_tables) if !inherited_tables.is_empty() => {
                write!(w, "\nINHERITS (")?;
                join_display_iter(inherited_tables.iter(), ",", w)?;
                write!(w, ")")?;
            }
            _ => {}
        }
        if let Some(partition_key_def) = &self.partition_key_def {
            write!(w, "\nPARTITION BY {partition_key_def}")?;
        }
        if let Some(storage_parameter) = &self.with {
            write!(w, "\nWITH (")?;
            join_display_iter(storage_parameter.iter(), ",", w)?;
            write!(w, ")'")?;
        }
        if let Some(tablespace) = &self.tablespace {
            write!(w, "\nTABLESPACE {}", tablespace)?;
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        match (&self.partition_key_def, &new.partition_key_def) {
            (Some(old_key), Some(new_key)) if old_key != new_key => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: self.name.to_string(),
                    reason: "Cannot update partition key definition".to_string(),
                })
            }
            _ => {}
        }

        match (&self.partition_values, &new.partition_values) {
            (Some(old_values), Some(new_values)) if old_values != new_values => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: self.name.to_string(),
                    reason: "Cannot update partition values".to_string(),
                })
            }
            _ => {}
        }

        match (
            &self.partitioned_parent_table,
            &new.partitioned_parent_table,
        ) {
            (Some(old_key), Some(new_key)) if old_key != new_key => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: self.name.to_string(),
                    reason: "Cannot update parent partition table".to_string(),
                })
            }
            _ => {}
        }

        if let Some(old_inherit) = &self.inherited_tables {
            let new_inherited = new.inherited_tables.as_ref();
            for remove_inherit in old_inherit
                .iter()
                .filter(|i| new_inherited.map(|o| o.contains(i)).unwrap_or(true))
            {
                writeln!(w, "ALTER TABLE {} NO INHERIT {remove_inherit};", self.name)?;
            }
        }
        if let Some(new_inherit) = &new.inherited_tables {
            let old_inherited = self.inherited_tables.as_ref();
            for add_inherit in new_inherit
                .iter()
                .filter(|i| old_inherited.map(|o| o.contains(i)).unwrap_or(true))
            {
                writeln!(w, "ALTER TABLE {} INHERIT {add_inherit};", self.name)?;
            }
        }

        for column in &self.columns {
            if let Some(other) = new.columns.iter().find(|c| c.name == column.name) {
                column.alter_column(other, self, w)?;
            } else {
                column.drop_column(self, w)?;
            };
        }
        for column in &new.columns {
            if !self.columns.iter().any(|c| c.name == column.name) {
                column.add_column(self, w)?;
            }
        }

        let compare_tablespace = TablespaceCompare {
            old: self.tablespace.as_ref(),
            new: new.tablespace.as_ref(),
        };
        if compare_tablespace.has_diff() {
            writeln!(w, "ALTER TABLE {} {compare_tablespace};", self.name)?;
        }
        compare_option_lists(
            self.object_type_name(),
            &self.name,
            self.with.as_deref(),
            new.with.as_deref(),
            w,
        )?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP TABLE {};", self.name)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Column {
    name: String,
    data_type: String,
    size: i32,
    collation: Option<Collation>,
    is_non_null: bool,
    default_expression: Option<String>,
    generated_column: Option<GeneratedColumn>,
    identity_column: Option<IdentityColumn>,
    storage: Option<Storage>,
    compression: Compression,
}

impl Column {
    fn field_definition<W: Write>(
        &self,
        include_storage: bool,
        w: &mut W,
    ) -> Result<(), std::fmt::Error> {
        write!(w, "{} {}", self.name, self.data_type)?;
        if include_storage {
            match &self.storage {
                Some(storage) if self.size != -1 && matches!(storage, Storage::Main) => {
                    write!(w, " {}", storage.as_ref())?;
                    write!(w, " {}", self.compression.as_ref())?;
                }
                Some(storage) if self.size == -1 && matches!(storage, Storage::External) => {
                    write!(w, " {}", storage.as_ref())?;
                    write!(w, " {}", self.compression.as_ref())?;
                }
                _ => {}
            }
        }
        match &self.collation {
            Some(collation) if !collation.is_default() => {
                write!(w, " {collation}")?;
            }
            _ => {}
        }
        write!(w, "{} NULL", if self.is_non_null { " NOT" } else { "" })?;
        if let Some(default_expression) = &self.default_expression {
            write!(w, " DEFAULT {default_expression}")?;
        }
        if let Some(generated_column) = &self.generated_column {
            write!(w, " {generated_column}")?;
        }
        if let Some(identity_column) = &self.identity_column {
            write!(w, " {identity_column}")?;
        }
        Ok(())
    }

    fn add_column<W: Write>(&self, table: &Table, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "ALTER TABLE {} ADD COLUMN ", table.name)?;
        self.field_definition(false, w)?;
        writeln!(w, ";")?;
        if let Some(storage) = &self.storage {
            writeln!(
                w,
                "\nALTER TABLE {} ALTER COLUMN {} SET {};",
                table.name,
                self.name,
                storage.as_ref()
            )?;
        }
        if !self.compression.as_ref().is_empty() {
            writeln!(
                w,
                "\nALTER TABLE {} ALTER COLUMN {} SET {};",
                table.name,
                self.name,
                self.compression.as_ref()
            )?;
        }
        Ok(())
    }

    fn drop_column<W: Write>(&self, table: &Table, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "\nALTER {} DROP COLUMN {};", table.name, self.name)?;
        Ok(())
    }

    fn alter_column<W: Write>(
        &self,
        other: &Self,
        table: &Table,
        w: &mut W,
    ) -> Result<(), PgDiffError> {
        if self.data_type != other.data_type {
            return Err(PgDiffError::InvalidMigration {
                object_name: table.name.to_string(),
                reason: format!("Attempted to change the data type of a column which is currently not supported. Column = {}", self.name),
            });
        }
        if self.is_non_null != other.is_non_null {
            writeln!(
                w,
                "ALTER TABLE {} ALTER COLUMN {} {};",
                table.name,
                self.name,
                if self.is_non_null {
                    "DROP NOT NULL"
                } else {
                    "SET NOT NULL"
                }
            )?;
        }
        match (&self.default_expression, &other.default_expression) {
            (Some(old_expression), Some(new_expression)) if old_expression != new_expression => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                    table.name, self.name
                )?;
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {new_expression};",
                    table.name, self.name
                )?;
            }
            (Some(_), None) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                    table.name, self.name
                )?;
            }
            (None, Some(new_expression)) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {new_expression};",
                    table.name, self.name
                )?;
            }
            _ => {}
        }
        match (&self.generated_column, &other.generated_column) {
            (Some(old_expression), Some(new_expression)) if old_expression != new_expression => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: table.name.to_string(),
                    reason: format!("Attempted to change the generation expression of a column ({}). This is not possible and you must create a new column.", self.name),
                })
            }
            (Some(_), None) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} DROP EXPRESSION;",
                    table.name,
                    self.name
                )?;
            }
            (None, Some(_)) => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: table.name.to_string(),
                    reason: format!("Attempted to add a generation expression to a column ({}). This is not possible and you must create a new column.", self.name),
                })
            }
            _ => {}
        }
        match (&self.identity_column, &other.identity_column) {
            (Some(old_identity), Some(new_identity)) if old_identity != new_identity => {
                if old_identity.identity_generation != new_identity.identity_generation {
                    writeln!(
                        w,
                        "ALTER TABLE {} ALTER COLUMN {} SET GENERATED {};",
                        table.name,
                        self.name,
                        new_identity.identity_generation.as_ref()
                    )?;
                }
                if old_identity.sequence_options != new_identity.sequence_options {
                    write!(
                        w,
                        "\nALTER TABLE {} ALTER COLUMN {} ",
                        table.name, self.name
                    )?;
                    new_identity.sequence_options.alter_sequence(w)?;
                    writeln!(w, ";")?;
                }
            }
            (Some(_), None) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} DROP IDENTITY;",
                    table.name, self.name
                )?;
            }
            (None, Some(new_identity)) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} ADD {new_identity};",
                    table.name, self.name
                )?;
            }
            _ => {}
        }
        match (&self.storage, &other.storage) {
            (Some(old_storage), Some(new_storage)) if old_storage != new_storage => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} SET {};",
                    table.name,
                    self.name,
                    new_storage.as_ref()
                )?;
            }
            _ => {}
        }
        if self.compression != other.compression {
            writeln!(
                w,
                "ALTER TABLE {} ALTER COLUMN {} SET {};",
                table.name,
                self.name,
                other.compression.as_ref()
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct GeneratedColumn {
    expression: String,
    generation_type: GeneratedColumnType,
}

impl Display for GeneratedColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            " GENERATED ALWAYS AS ({}) {}",
            self.expression,
            self.generation_type.as_ref()
        )
    }
}

#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum GeneratedColumnType {
    #[strum(serialize = "STORED")]
    Stored,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IdentityColumn {
    identity_generation: IdentityGeneration,
    sequence_options: SequenceOptions,
}

impl Display for IdentityColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GENERATED {} AS IDENTITY ({})",
            self.identity_generation.as_ref(),
            self.sequence_options
        )
    }
}

#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum IdentityGeneration {
    #[strum(serialize = "ALWAYS")]
    Always,
    #[strum(serialize = "DEFAULT")]
    Default,
}

#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum Storage {
    #[serde(alias = "p")]
    #[strum(serialize = "STORAGE PLAIN")]
    Plain,
    #[serde(alias = "e")]
    #[strum(serialize = "STORAGE EXTERNAL")]
    External,
    #[serde(alias = "m")]
    #[strum(serialize = "STORAGE MAIN")]
    Main,
    #[serde(alias = "x")]
    #[strum(serialize = "STORAGE EXTENDED")]
    Extended,
}

#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum Compression {
    #[serde(alias = "")]
    #[strum(serialize = "")]
    Default,
    #[serde(alias = "p")]
    #[strum(serialize = "COMPRESSION pglz")]
    PGLZ,
    #[serde(alias = "l")]
    #[strum(serialize = "COMPRESSION lz4")]
    LZ4,
}

#[derive(Debug, Deserialize, PartialEq, sqlx::FromRow)]
pub struct Constraint {
    table_oid: Oid,
    #[sqlx(json)]
    owner_table_name: SchemaQualifiedName,
    name: String,
    #[sqlx(json)]
    schema_qualified_name: SchemaQualifiedName,
    #[sqlx(json)]
    constraint_type: ConstraintType,
    #[sqlx(json)]
    timing: ConstraintTiming,
}

impl SqlObject for Constraint {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "CONSTRAINT"
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
                join_slice(columns, ",", w)?;
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
                join_slice(columns, ",", w)?;
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
                join_slice(columns, ",", w)?;
                write!(w, ") REFERENCES {ref_table}(")?;
                join_slice(ref_columns, ",", w)?;
                write!(
                    w,
                    ") {}\n\tON DELETE {on_delete}\n\tON UPDATE {on_update}",
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
                    join_slice(columns, ",", f)?;
                    write!(f, ")")
                } else {
                    write!(f, "SET NULL")
                }
            }
            ForeignKeyAction::SetDefault { columns } => {
                if let Some(columns) = columns {
                    write!(f, "SET DEFAULT (")?;
                    join_slice(columns, ",", f)?;
                    write!(f, ")")
                } else {
                    write!(f, "SET DEFAULT")
                }
            }
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Index {
    table_oid: Oid,
    #[sqlx(json)]
    owner_table_name: SchemaQualifiedName,
    #[sqlx(json)]
    schema_qualified_name: SchemaQualifiedName,
    columns: Vec<String>,
    is_valid: bool,
    definition_statement: String,
    #[sqlx(flatten)]
    parameters: IndexParameters,
}

impl PartialEq for Index {
    fn eq(&self, other: &Self) -> bool {
        self.definition_statement == other.definition_statement
    }
}

impl SqlObject for Index {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "INDEX"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "{};", self.definition_statement)?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.columns == new.columns
            && self.parameters.include == new.parameters.include
            && self.parameters.with != new.parameters.with
        {
            compare_option_lists(
                "INDEX",
                &self.schema_qualified_name,
                self.parameters.with.as_deref(),
                new.parameters.with.as_deref(),
                w,
            )?;
            let compare_tablespace = TablespaceCompare {
                old: self.parameters.tablespace.as_ref(),
                new: new.parameters.tablespace.as_ref(),
            };
            if compare_tablespace.has_diff() {
                writeln!(
                    w,
                    "ALTER INDEX {} {compare_tablespace};",
                    self.schema_qualified_name,
                )?;
            }
            return Ok(());
        }

        self.drop_statements(w)?;
        self.create_statements(w)?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP INDEX {};", self.schema_qualified_name)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct StorageParameter(String);

impl Display for StorageParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn compare_option_lists<O, W>(
    object_type_name: &str,
    object_name: &SchemaQualifiedName,
    old: Option<&[O]>,
    new: Option<&[O]>,
    w: &mut W,
) -> Result<(), PgDiffError>
where
    O: Display + PartialEq,
    W: Write,
{
    if let Some(new_options) = new {
        let old_options = old.unwrap_or_default();
        let set_options = new_options.iter().filter(|p| old_options.contains(*p));
        write!(w, "ALTER {} {} SET (", object_type_name, object_name)?;
        join_display_iter(set_options, ",", w)?;
        writeln!(w, ");")?;
    }
    if let Some(old_options) = old {
        let new_options = new.unwrap_or_default();
        write!(w, "ALTER {} {} RESET (", object_type_name, object_name)?;
        for p in old_options.iter().filter(|p| new_options.contains(*p)) {
            let option = p.to_string();
            if let Some((first, _)) = option.split_once('=') {
                write!(w, "{first}")?;
            } else {
                write!(w, "{option}")?;
            }
        }
        writeln!(w, ");")?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Deserialize, sqlx::FromRow)]
pub struct IndexParameters {
    include: Option<Vec<String>>,
    with: Option<Vec<StorageParameter>>,
    tablespace: Option<TableSpace>,
}

impl Display for IndexParameters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.include {
            Some(include) if !include.is_empty() => {
                write!(f, " INCLUDE(")?;
                join_slice(include.as_slice(), ",", f)?;
                write!(f, ")")?;
            }
            _ => {}
        }
        match &self.with {
            Some(storage_parameters) if !storage_parameters.is_empty() => {
                write!(f, " WITH(")?;
                map_join_slice(
                    storage_parameters.as_slice(),
                    |p, f| {
                        write!(f, "{p}")?;
                        Ok(())
                    },
                    ",",
                    f,
                )?;
                write!(f, ")")?;
            }
            _ => {}
        }
        if let Some(tablespace) = &self.tablespace {
            write!(f, " USING INDEX TABLESPACE {}", tablespace)?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Policy {
    name: String,
    schema_qualified_name: SchemaQualifiedName,
    owner_table: SchemaQualifiedName,
    is_permissive: bool,
    applies_to: Vec<String>,
    command: PolicyCommand,
    check_expression: Option<String>,
    using_expression: Option<String>,
    columns: Vec<String>,
}

impl SqlObject for Policy {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "POLICY"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(
            w,
            "CREATE POLICY {} ON {} AS {} FOR {} TO {}",
            self.name,
            self.owner_table,
            if self.is_permissive {
                "PERMISSIVE"
            } else {
                "RESTRICTIVE"
            },
            self.command.as_ref(),
            self.applies_to.join(" ")
        )?;
        if let Some(using_expression) = &self.using_expression {
            write!(w, " USING ({using_expression})")?;
        }
        if let Some(check_expression) = &self.check_expression {
            write!(w, " WITH CHECK ({check_expression})")?;
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.is_permissive != new.is_permissive || self.command != new.command {
            self.drop_statements(w)?;
            self.create_statements(w)?;
            return Ok(());
        }
        write!(
            w,
            "ALTER POLICY {} ON {} TO {}",
            self.name,
            self.owner_table,
            new.applies_to.join(" ")
        )?;
        if let Some(using_expression) = &new.using_expression {
            write!(w, " USING ({using_expression})")?;
        }
        if let Some(check_expression) = &new.check_expression {
            write!(w, " WITH CHECK ({check_expression})")?;
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP POLICY {} ON {};", self.name, self.owner_table)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum PolicyCommand {
    #[serde(rename = "r")]
    #[strum(serialize = "SELECT")]
    Select,
    #[serde(rename = "a")]
    #[strum(serialize = "INSERT")]
    Insert,
    #[serde(rename = "w")]
    #[strum(serialize = "UPDATE")]
    Update,
    #[serde(rename = "d")]
    #[strum(serialize = "DELETE")]
    Delete,
    #[serde(rename = "*")]
    #[strum(serialize = "ALL")]
    All,
}

#[derive(Debug, PartialEq)]
pub struct Sequence {
    name: SchemaQualifiedName,
    data_type: String,
    owner: Option<SequenceOwner>,
    sequence_options: SequenceOptions,
}

impl<'r> FromRow<'r, PgRow> for Sequence {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let data_type = row.try_get("data_type")?;
        let owner: Option<Json<SequenceOwner>> = row.try_get("owner")?;
        let sequence_options: Json<SequenceOptions> = row.try_get("sequence_options")?;
        Ok(Self {
            name: name.0,
            data_type,
            owner: owner.map(|j| j.0),
            sequence_options: sequence_options.0,
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

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(
            w,
            "CREATE SEQUENCE {} AS {} {}",
            self.name, self.data_type, self.sequence_options,
        )?;
        if let Some(owner) = &self.owner {
            writeln!(w, " {owner};")?;
        } else {
            writeln!(w, " OWNED BY NONE;")?;
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
                write!(w, " OWNED BY NONE")?;
            }
            (None, Some(new_owner)) => {
                write!(w, " OWNED BY {new_owner}")?;
            }
            _ => {}
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP SEQUENCE {};", self.name)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct SequenceOptions {
    increment: i64,
    min_value: i64,
    max_value: i64,
    start_value: i64,
    cache: i64,
    is_cycle: bool,
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
    fn alter_sequence<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
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
    table_name: SchemaQualifiedName,
    column_name: String,
}

impl Display for SequenceOwner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "OWNED BY {}.{}", self.table_name, self.column_name)
    }
}

#[derive(Debug, PartialEq)]
pub struct Function {
    name: SchemaQualifiedName,
    is_procedure: bool,
    signature: String,
    definition: String,
    language: String,
    function_dependencies: Option<Vec<FunctionDependency>>,
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
    name: SchemaQualifiedName,
    signature: String,
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Trigger {
    table_oid: Oid,
    name: String,
    #[sqlx(json)]
    schema_qualified_name: SchemaQualifiedName,
    #[sqlx(json)]
    owner_table_name: SchemaQualifiedName,
    timing: TriggerTiming,
    #[sqlx(json)]
    events: Vec<TriggerEvent>,
    old_name: Option<String>,
    new_name: Option<String>,
    is_row_level: bool,
    when_expression: Option<String>,
    #[sqlx(json)]
    function_name: SchemaQualifiedName,
    function_args: Option<Vec<u8>>,
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

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct View {
    #[sqlx(json)]
    name: SchemaQualifiedName,
    columns: Option<Vec<String>>,
    query: String,
    options: Option<Vec<String>>,
}

impl SqlObject for View {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "VIEW"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "CREATE OR REPLACE VIEW {}", self.name)?;
        if let Some(columns) = &self.columns {
            write!(w, "('")?;
            join_slice(columns.as_slice(), ",", w)?;
            write!(w, ")'")?;
        }
        if let Some(options) = &self.options {
            write!(w, "('")?;
            join_slice(options.as_slice(), ",", w)?;
            write!(w, ")'")?;
        }
        writeln!(w, " AS\n{};", self.query)?;
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
        compare_option_lists(
            self.object_type_name(),
            &self.name,
            self.options.as_deref(),
            new.options.as_deref(),
            w,
        )?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP VIEW {};", self.name)?;
        Ok(())
    }
}

async fn get_database(pool: &PgPool) -> Result<Database, PgDiffError> {
    let schemas = get_schemas(pool).await?;
    let schema_names: Vec<&str> = schemas
        .iter()
        .map(|s| s.name.schema_name.as_str())
        .collect();
    let udts = get_udts(pool, &schema_names).await?;
    let tables = get_tables(pool, &schema_names).await?;
    let table_oids: Vec<Oid> = tables.iter().map(|t| t.oid).collect();
    let constraints = get_constraints(pool, &table_oids).await?;
    let indexes = get_indexes(pool, &table_oids).await?;
    let triggers = get_triggers(pool, &table_oids).await?;
    let sequences = get_sequences(pool, &schema_names).await?;
    let functions = get_functions(pool, &schema_names).await?;
    let views = get_views(pool, &schema_names).await?;
    Ok(Database {
        schemas,
        udts,
        tables,
        constraints,
        indexes,
        triggers,
        sequences,
        functions,
        views,
        extensions: get_extensions(pool).await?,
    })
}

async fn get_schemas(pool: &PgPool) -> Result<Vec<Schema>, PgDiffError> {
    let schemas_query = include_str!("./../queries/schemas.pgsql");
    let schema_names = match query_as(schemas_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load schemas");
            return Err(error.into());
        }
    };
    Ok(schema_names)
}

async fn get_udts(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Udt>, PgDiffError> {
    let udts_query = include_str!("./../queries/udts.pgsql");
    let udts = match query_as(udts_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load udts");
            return Err(error.into());
        }
    };
    Ok(udts)
}

async fn get_tables(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Table>, PgDiffError> {
    let tables_query = include_str!("./../queries/tables.pgsql");
    let tables = match query_as(tables_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load tables");
            return Err(error.into());
        }
    };
    Ok(tables)
}

async fn get_constraints(pool: &PgPool, tables: &[Oid]) -> Result<Vec<Constraint>, PgDiffError> {
    let constraints_query = include_str!("./../queries/constraints.pgsql");
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

async fn get_indexes(pool: &PgPool, tables: &[Oid]) -> Result<Vec<Index>, PgDiffError> {
    let indexes_query = include_str!("./../queries/indexes.pgsql");
    let indexes = match query_as(indexes_query).bind(tables).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load index");
            return Err(error.into());
        }
    };
    Ok(indexes)
}

async fn get_triggers(pool: &PgPool, tables: &[Oid]) -> Result<Vec<Trigger>, PgDiffError> {
    let triggers_query = include_str!("./../queries/triggers.pgsql");
    let triggers = match query_as(triggers_query).bind(tables).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load triggers");
            return Err(error.into());
        }
    };
    Ok(triggers)
}

async fn get_sequences(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Sequence>, PgDiffError> {
    let sequence_query = include_str!("./../queries/sequences.pgsql");
    let sequences = match query_as(sequence_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load sequences");
            return Err(error.into());
        }
    };
    Ok(sequences)
}

async fn get_functions(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Function>, PgDiffError> {
    let functions_query = include_str!("../queries/functions.pgsql");
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

async fn get_views(pool: &PgPool, schemas: &[&str]) -> Result<Vec<View>, PgDiffError> {
    let views_query = include_str!("./../queries/views.pgsql");
    let views = match query_as(views_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load views");
            return Err(error.into());
        }
    };
    Ok(views)
}

async fn get_extensions(pool: &PgPool) -> Result<Vec<Extension>, PgDiffError> {
    let extensions_query = include_str!("./../queries/extensions.pgsql");
    let extensions = match query_as(extensions_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load extensions");
            return Err(error.into());
        }
    };
    Ok(extensions)
}

/// Write create statements to file
async fn write_create_statements_to_file<S, P>(
    object: &S,
    root_directory: P,
) -> Result<(), PgDiffError>
where
    S: SqlObject,
    P: AsRef<Path>,
{
    let mut statements = String::new();
    object.create_statements(&mut statements)?;

    let path = root_directory
        .as_ref()
        .join(object.object_type_name().to_lowercase());
    tokio::fs::create_dir_all(&path).await?;
    let mut file = File::create(path.join(format!("{}.pgsql", object.name()))).await?;
    file.write_all(statements.as_bytes()).await?;
    Ok(())
}

async fn append_create_statements_table_to_file<S, P>(
    object: &S,
    owner_table: &SchemaQualifiedName,
    root_directory: P,
) -> Result<(), PgDiffError>
where
    S: SqlObject,
    P: AsRef<Path>,
{
    let mut statements = String::new();
    object.create_statements(&mut statements)?;

    let path = root_directory.as_ref().join("table");
    tokio::fs::create_dir_all(&path).await?;
    let mut file = OpenOptions::new()
        .append(true)
        .open(path.join(format!("{}.pgsql", owner_table)))
        .await?;
    file.write_all("\n".as_bytes()).await?;
    file.write_all(statements.as_bytes()).await?;
    Ok(())
}

#[derive(Debug, Parser)]
#[command(
    version = "0.0.1",
    about = "Postgresql schema diffing and migration tool",
    long_about = None
)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(
        version = "0.0.1",
        about = "Script the target database of all relevant SQL objects",
        long_about = None
    )]
    Script {
        #[arg(short, long)]
        connection: String,
        #[arg(short = 'o', long)]
        output_path: PathBuf,
    },
    #[command(
        version = "0.0.1",
        about = "Perform the required migration steps to upgrade the target database to the objects described in the source files",
        long_about = None
    )]
    Migrate {
        #[arg(short, long)]
        connection: String,
        #[arg(short = 'p', long)]
        files_path: PathBuf,
    },
    #[command(
        version = "0.0.1",
        about = "Plan (but does not execute!) the required migration steps to upgrade the target database to the objects in the source files",
        long_about = None
    )]
    Plan {
        #[arg(short, long)]
        connection: String,
        #[arg(short = 'p', long)]
        files_path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), PgDiffError> {
    let args = Args::parse();

    match &args.command {
        Commands::Script {
            output_path,
            connection,
        } => {
            let mut connect_options = PgConnectOptions::from_str(connection)?;
            if let Ok(password) = std::env::var("PGPASSWORD") {
                connect_options = connect_options.password(&password);
            }
            let pool = PgPool::connect_with(connect_options).await?;
            let database = get_database(&pool).await?;
            for schema in &database.schemas {
                write_create_statements_to_file(schema, &output_path).await?;
            }
            for udt in &database.udts {
                write_create_statements_to_file(udt, &output_path).await?;
            }
            for table in &database.tables {
                write_create_statements_to_file(table, &output_path).await?;
                for constraint in database
                    .constraints
                    .iter()
                    .filter(|c| c.table_oid == table.oid)
                {
                    append_create_statements_table_to_file(constraint, &constraint.owner_table_name, &output_path).await?
                }
                for index in database.indexes.iter().filter(|i| i.table_oid == table.oid) {
                    append_create_statements_table_to_file(index, &index.owner_table_name, &output_path).await?
                }
                for trigger in database
                    .triggers
                    .iter()
                    .filter(|t| t.table_oid == table.oid)
                {
                    append_create_statements_table_to_file(trigger, &trigger.owner_table_name, &output_path).await?
                }
            }
            for sequence in &database.sequences {
                if let Some(owner_table) = &sequence.owner {
                    append_create_statements_table_to_file(sequence, &owner_table.table_name, &output_path).await?;
                } else {
                    write_create_statements_to_file(sequence, &output_path).await?;
                }
            }
            for function in &database.functions {
                write_create_statements_to_file(function, &output_path).await?;
            }
            for view in &database.views {
                write_create_statements_to_file(view, &output_path).await?;
            }
            for extension in &database.extensions {
                write_create_statements_to_file(extension, &output_path).await?;
            }
        }
        Commands::Migrate { .. } => {}
        Commands::Plan { .. } => {}
    }
    // println!("{database:?}");
    Ok(())
}
