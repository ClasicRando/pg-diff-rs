use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{query_as, FromRow, PgPool, Row};

use crate::{map_join_slice, write_join, PgDiffError};

use super::sequence::SequenceOptions;
use super::{
    check_names_in_database, compare_tablespaces, Collation, OptionListObject, SchemaQualifiedName,
    SqlObject, StorageParameter, TableSpace,
};

/// Fetch all tables that are found in the specified schemas.
pub async fn get_tables(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Table>, PgDiffError> {
    let tables_query = include_str!("./../../queries/tables.pgsql");
    let tables = match query_as(tables_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load tables");
            return Err(error.into());
        },
    };
    Ok(tables)
}

/// Fetch all tables that could be associated with the provided qualified name
pub async fn get_table_by_qualified_name(
    pool: &PgPool,
    schema_qualified_name: &SchemaQualifiedName,
) -> Result<Vec<SchemaQualifiedName>, PgDiffError> {
    let tables_query = include_str!("./../../queries/dependency_tables.pgsql");
    let tables = match check_names_in_database(pool, schema_qualified_name, tables_query).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load tables by qualified name");
            return Err(error.into());
        },
    };
    Ok(tables)
}

/// Struct representing a SQL table
#[derive(Debug)]
pub struct Table {
    /// OID of the table
    pub(crate) oid: Oid,
    /// Full name of the table
    pub(crate) name: SchemaQualifiedName,
    /// Columns of the table
    pub(crate) columns: Vec<Column>,
    /// The partition key definition if this table is a parent partitioned table
    pub(crate) partition_key_def: Option<String>,
    /// The partition key values if this table is a partition of another table
    pub(crate) partition_values: Option<String>,
    /// All parent tables if this table inherits from a parent table. Different from partitioned
    /// tables.
    pub(crate) inherited_tables: Option<Vec<SchemaQualifiedName>>,
    /// The parent partitioned table if this is a partition of another table
    pub(crate) partitioned_parent_table: Option<SchemaQualifiedName>,
    /// Optional tablespace to store this table. [None] means the default tablespace is used.
    pub(crate) tablespace: Option<TableSpace>,
    /// Optional storage parameters for this table
    pub(crate) with: Option<Vec<StorageParameter>>,
    /// Dependencies of this table
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for Table {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.columns == other.columns
            && self.partition_key_def == other.partition_key_def
            && self.partition_values == other.partition_values
            && self.inherited_tables == other.inherited_tables
            && self.partitioned_parent_table == other.partitioned_parent_table
            && self.tablespace == other.tablespace
            && self.with == other.with
            && self.dependencies == other.dependencies
    }
}

impl<'r> FromRow<'r, PgRow> for Table {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let oid: Oid = row.try_get("oid")?;
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let columns: Json<Vec<Column>> = row.try_get("columns")?;
        let partition_key_def: Option<String> = row.try_get("partition_key_def")?;
        let partition_values: Option<String> = row.try_get("partition_values")?;
        let inherited_tables: Option<Json<Vec<SchemaQualifiedName>>> =
            row.try_get("inherited_tables")?;
        let partitioned_parent_table: Option<Json<SchemaQualifiedName>> =
            row.try_get("partitioned_parent_table")?;
        let tablespace: Option<TableSpace> = row.try_get("tablespace")?;
        let with: Option<Vec<StorageParameter>> = row.try_get("with")?;
        let dependencies: Json<Vec<SchemaQualifiedName>> = row.try_get("dependencies")?;
        Ok(Self {
            oid,
            name: name.0,
            columns: columns.0,
            partition_key_def,
            partition_values,
            inherited_tables: inherited_tables.map(|j| j.0),
            partitioned_parent_table: partitioned_parent_table.map(|j| j.0),
            tablespace,
            with,
            dependencies: dependencies.0,
        })
    }
}

impl OptionListObject for Table {}

impl SqlObject for Table {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        "TABLE"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "CREATE TABLE {}", self.name)?;
        if let Some(partitioned_parent_table) = &self.partitioned_parent_table {
            write!(w, "PARTITION OF {partitioned_parent_table}")?;
        } else if !self.columns.is_empty() {
            w.write_str("(\n    ")?;
            map_join_slice(
                self.columns.as_slice(),
                |c, s| c.field_definition(true, s),
                ",\n    ",
                w,
            )?;
            w.write_str("\n)")?;
        }
        match &self.partition_values {
            Some(partition_values) => {
                write!(w, "\nFOR VALUES {partition_values}")?;
            },
            None if self.partitioned_parent_table.is_some() => {
                w.write_str("\nDEFAULT")?;
            },
            _ => {},
        }
        match &self.inherited_tables {
            Some(inherited_tables) if !inherited_tables.is_empty() => {
                w.write_str("\nINHERITS (")?;
                write_join!(w, inherited_tables, ",");
                w.write_str(")")?;
            },
            _ => {},
        }
        if let Some(partition_key_def) = &self.partition_key_def {
            write!(w, "\nPARTITION BY {partition_key_def}")?;
        }
        if let Some(storage_parameter) = &self.with {
            w.write_str("\nWITH (")?;
            write_join!(w, storage_parameter, ",");
            w.write_str(")")?;
        }
        if let Some(tablespace) = &self.tablespace {
            write!(w, "\nTABLESPACE {}", tablespace)?;
        }
        w.write_str(";\n")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        match (&self.partition_key_def, &new.partition_key_def) {
            (Some(old_key), Some(new_key)) if old_key != new_key => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: self.name.to_string(),
                    reason: "Cannot update partition key definition".to_string(),
                })
            },
            _ => {},
        }

        match (&self.partition_values, &new.partition_values) {
            (Some(old_values), Some(new_values)) if old_values != new_values => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: self.name.to_string(),
                    reason: "Cannot update partition values".to_string(),
                })
            },
            _ => {},
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
            },
            _ => {},
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

        compare_tablespaces(self.tablespace.as_ref(), new.tablespace.as_ref(), w)?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP TABLE {};", self.name)?;
        Ok(())
    }
}

/// Struct representing a SQL table column
#[derive(Debug, Deserialize, PartialEq)]
pub struct Column {
    /// Column name
    name: String,
    /// Data type of the column
    data_type: String,
    /// Size in bytes of the column. If the size is variable (e.g. varchar) then -1.
    size: i32,
    /// Collation of the type if it's a character type, otherwise [None]
    collation: Option<Collation>,
    /// True if the column has a `NOT NULL` constraint
    is_non_null: bool,
    /// Optional default value constraint on the column. If present, the value is the expression
    default_expression: Option<String>,
    /// Contains value generation details if the column is generated by an expression
    generated_column: Option<GeneratedColumn>,
    /// Contains identity details if the column is an identity
    identity_column: Option<IdentityColumn>,
    /// Optional storage details for the column
    storage: Option<Storage>,
    /// Compression option for the column
    compression: Compression,
}

impl Column {
    /// Write a field definition to a writable object. If `include_storage` is true, storage and
    /// compression details are included. This is only true for generating a `CREATE` statement.
    fn field_definition<W: Write>(
        &self,
        include_storage: bool,
        w: &mut W,
    ) -> Result<(), std::fmt::Error> {
        write!(w, "{} {}", self.name, self.data_type)?;
        if include_storage && self.size != -1 {
            if let Some(storage) = &self.storage {
                match storage {
                    Storage::Main | Storage::Extended => {
                        write!(w, " {}", storage.as_ref())?;
                        write!(w, " {}", self.compression.as_ref())?;
                    },
                    _ => {},
                }
            }
        }
        match &self.collation {
            Some(collation) if !collation.is_default() => {
                write!(w, " {collation}")?;
            },
            _ => {},
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

    /// Write an `ALTER TABLE {} ADD COLUMN` statement for this column to the writeable object
    fn add_column<W: Write>(&self, table: &Table, w: &mut W) -> Result<(), PgDiffError> {
        write!(w, "ALTER TABLE {} ADD COLUMN ", table.name)?;
        self.field_definition(false, w)?;
        w.write_str(";\n")?;
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

    /// Write an `ALTER TABLE {} DROP COLUMN` statement for this column to the writeable object
    fn drop_column<W: Write>(&self, table: &Table, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "\nALTER {} DROP COLUMN {};", table.name, self.name)?;
        Ok(())
    }

    /// Write an `ALTER TABLE {} ALTER COLUMN` statement for this column to the writeable object
    ///
    /// ## Errors
    /// - if the data type of the column has changed between migrations
    /// - if the column becomes a generated column
    /// - if the column has a new generation expression
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
            },
            (Some(_), None) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT;",
                    table.name, self.name
                )?;
            },
            (None, Some(new_expression)) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {new_expression};",
                    table.name, self.name
                )?;
            },
            _ => {},
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
                    w.write_str(";\n")?;
                }
            },
            (Some(_), None) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} DROP IDENTITY;",
                    table.name, self.name
                )?;
            },
            (None, Some(new_identity)) => {
                writeln!(
                    w,
                    "ALTER TABLE {} ALTER COLUMN {} ADD {new_identity};",
                    table.name, self.name
                )?;
            },
            _ => {},
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
            },
            _ => {},
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

/// Column generation details
#[derive(Debug, Deserialize, PartialEq)]
pub struct GeneratedColumn {
    /// Raw expression used to generate a column value
    expression: String,
    /// Generation option. Currently only `STORED` is supported
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

/// Column Generation strategy. Currently only `STORED` is supported
#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum GeneratedColumnType {
    /// Generated column is stored with the owning record
    #[strum(serialize = "STORED")]
    Stored,
}

/// Identity column details
#[derive(Debug, Deserialize, PartialEq)]
pub struct IdentityColumn {
    /// Generation strategy
    identity_generation: IdentityGeneration,
    /// Identity sequence options
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

/// Identity generation strategy variant
#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum IdentityGeneration {
    /// Identity value is always generated
    #[strum(serialize = "ALWAYS")]
    Always,
    /// Identity value is used when an explicit value is not provided
    #[strum(serialize = "DEFAULT")]
    Default,
}

/// Column storage variants
#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum Storage {
    /// Storage for fixed-length values such as `integer`. This must be used for fixed-length
    /// values.
    #[serde(alias = "p")]
    #[strum(serialize = "STORAGE PLAIN")]
    Plain,
    /// Storage for variable length values where the data is stored externally with no compression
    #[serde(alias = "e")]
    #[strum(serialize = "STORAGE EXTERNAL")]
    External,
    /// Storage for variable length values where the data is stored inline with the record
    #[serde(alias = "m")]
    #[strum(serialize = "STORAGE MAIN")]
    Main,
    /// Storage for variable length values where the data is stored externally with compression.
    /// This is the default for variable length data.
    #[serde(alias = "x")]
    #[strum(serialize = "STORAGE EXTENDED")]
    Extended,
}

/// Compression option variants for a column
#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum Compression {
    /// Default compression is used for a column. This should always be treated as empty since this
    /// should never display anything for columns not compressed.
    #[serde(alias = "")]
    #[strum(serialize = "")]
    Default,
    /// Postgres' custom LZ compression method
    #[serde(alias = "p")]
    #[strum(serialize = "COMPRESSION pglz")]
    PGLZ,
    /// LZ4 compression method. Only available when postgres is built with special flag.
    #[serde(alias = "l")]
    #[strum(serialize = "COMPRESSION lz4")]
    LZ4,
}
