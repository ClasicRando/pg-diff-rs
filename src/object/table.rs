use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{query_as, FromRow, PgPool, Row};

use crate::{map_join_slice, write_join, PgDiffError};

use super::sequence::SequenceOptions;
use super::{compare_option_lists, Collation, Dependency, OptionListObject, PgCatalog, SchemaQualifiedName, SqlObject, StorageParameter, TableSpace, TablespaceCompare, GenericObject};

pub async fn get_tables(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Table>, PgDiffError> {
    let tables_query = include_str!("./../../queries/tables.pgsql");
    let tables = match query_as(tables_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load tables");
            return Err(error.into());
        }
    };
    Ok(tables)
}

pub async fn get_table_by_qualified_name(
    pool: &PgPool,
    schema_qualified_name: &SchemaQualifiedName,
) -> Result<Vec<GenericObject>, PgDiffError> {
    let tables_query = include_str!("./../../queries/dependency_tables.pgsql");
    let schema_specified = !schema_qualified_name.schema_name.is_empty();
    let schemas = if schema_specified {
        [&schema_qualified_name.schema_name, ""]
    } else {
        ["public", "pg_catalog"]
    };
    let tables = match query_as(tables_query).bind(schemas).bind(&schema_qualified_name.local_name).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load tables by qualified name");
            return Err(error.into());
        }
    };
    Ok(tables)
}

#[derive(Debug, PartialEq)]
pub struct Table {
    pub(crate) oid: Oid,
    pub(crate) name: SchemaQualifiedName,
    pub(crate) columns: Vec<Column>,
    pub(crate) partition_key_def: Option<String>,
    pub(crate) partition_values: Option<String>,
    pub(crate) inherited_tables: Option<Vec<SchemaQualifiedName>>,
    pub(crate) partitioned_parent_table: Option<SchemaQualifiedName>,
    pub(crate) tablespace: Option<TableSpace>,
    pub(crate) with: Option<Vec<StorageParameter>>,
    pub(crate) dependencies: Vec<Dependency>,
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
        let dependencies: Json<Vec<Dependency>> = row.try_get("dependencies")?;
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
        writeln!(w, "CREATE TABLE {}", self.name)?;
        if let Some(partitioned_parent_table) = &self.partitioned_parent_table {
            write!(w, "PARTITION OF {partitioned_parent_table}")?;
        } else if !self.columns.is_empty() {
            w.write_str("(\n\t")?;
            map_join_slice(
                self.columns.as_slice(),
                |c, s| c.field_definition(true, s),
                ",\n\t",
                w,
            )?;
            w.write_str("\n)")?;
        }
        match &self.partition_values {
            Some(partition_values) => {
                write!(w, "\nFOR VALUES {partition_values}")?;
            }
            None if self.partitioned_parent_table.is_some() => {
                w.write_str("\nDEFAULT")?;
            }
            _ => {}
        }
        match &self.inherited_tables {
            Some(inherited_tables) if !inherited_tables.is_empty() => {
                w.write_str("\nINHERITS (")?;
                write_join!(w, inherited_tables.iter(), ",");
                w.write_str(")")?;
            }
            _ => {}
        }
        if let Some(partition_key_def) = &self.partition_key_def {
            write!(w, "\nPARTITION BY {partition_key_def}")?;
        }
        if let Some(storage_parameter) = &self.with {
            w.write_str("\nWITH (")?;
            write_join!(w, storage_parameter.iter(), ",");
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

        let compare_tablespace =
            TablespaceCompare::new(self.tablespace.as_ref(), new.tablespace.as_ref());
        if compare_tablespace.has_diff() {
            writeln!(w, "ALTER TABLE {} {compare_tablespace};", self.name)?;
        }
        compare_option_lists(self, self.with.as_deref(), new.with.as_deref(), w)?;
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
                    w.write_str(";\n")?;
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
