use std::convert::Into;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use itertools::Itertools;
use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{PgConnectOptions, PgRow};
use sqlx::types::Json;
use sqlx::{query_as, query_scalar, Error as SqlxError, FromRow, PgPool, Row};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum PgDiffError {
    #[error(transparent)]
    Sql(#[from] SqlxError),
    #[error("{0}")]
    General(String),
    #[error("For {name}, found new type '{new_type}' that is incompatible with existing type {original_type}")]
    IncompatibleTypes {
        name: SchemaQualifiedName,
        original_type: String,
        new_type: String,
    },
    #[error("Could not construct a migration strategy for {type_name}. {reason}")]
    InvalidMigration { type_name: String, reason: String },
}

trait SqlObject {
    fn create_statement(&self) -> String;
    fn alter_statement(&self, other: &Self) -> Result<String, PgDiffError>;
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
    sequences: Vec<Sequence>,
    functions: Vec<Function>,
    triggers: Vec<Trigger>,
    views: Vec<View>,
    extensions: Vec<Extension>,
}

#[derive(Debug, sqlx::Type)]
#[sqlx(transparent)]
pub struct Schema(String);

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Deserialize)]
pub struct SchemaQualifiedName {
    schema_name: String,
    local_name: String,
}

impl Display for SchemaQualifiedName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\".\"{}\"", self.schema_name, self.local_name)
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct Extension {
    name: String,
    version: String,
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Udt {
    #[sqlx(json)]
    name: SchemaQualifiedName,
    #[sqlx(json)]
    udt_type: UdtType,
}

impl SqlObject for Udt {
    fn create_statement(&self) -> String {
        match &self.udt_type {
            UdtType::Enum { labels } => {
                format!(
                    "CREATE TYPE {} AS ENUM (\n'{}'\n);",
                    self.name,
                    labels.join("',\n\t'")
                )
            }
            UdtType::Composite { attributes } => {
                format!(
                    "CREATE TYPE {} AS (\n{}\n);",
                    self.name,
                    attributes
                        .iter()
                        .map(|f| f.field_definition())
                        .join(",\n\t")
                )
            }
            UdtType::Range { subtype } => {
                format!(
                    "CREATE TYPE {} AS RANGE (SUBTYPE = {});",
                    self.name, subtype
                )
            }
        }
    }

    fn alter_statement(&self, other: &Self) -> Result<String, PgDiffError> {
        if self.udt_type != other.udt_type {
            return Err(PgDiffError::IncompatibleTypes {
                name: self.name.clone(),
                original_type: self.udt_type.name().into(),
                new_type: other.udt_type.name().into(),
            });
        }
        match (&self.udt_type, &other.udt_type) {
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
                        type_name: self.name.to_string(),
                        reason: format!(
                            "Enum has values removed during migration. Missing values: '{:?}'",
                            missing_labels
                        ),
                    });
                }

                let new_labels: String = new_labels
                    .iter()
                    .filter(|label| !existing_labels.contains(*label))
                    .map(|label| format!("ALTER TYPE {} ADD VALUE '{label}';", self.name))
                    .join("\n");
                Ok(new_labels)
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
                        type_name: self.name.to_string(),
                        reason: format!(
                            "Composite has attributes removed during migration. Missing attributes: '{:?}'",
                            missing_attributes
                        ),
                    });
                }

                let new_attributes: String = new_attributes
                    .iter()
                    .filter(|attribute| {
                        !existing_attributes.iter().any(|a| attribute.name == a.name)
                    })
                    .map(|attribute| {
                        format!(
                            "ALTER TYPE {} ADD ATTRIBUTE {} {}{};",
                            self.name,
                            attribute.name,
                            attribute.data_type,
                            attribute
                                .collation
                                .as_ref()
                                .map(|c| format!("COLLATE {c}"))
                                .unwrap_or_default()
                        )
                    })
                    .join("\n");
                Ok(new_attributes)
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
                        type_name: self.name.to_string(),
                        reason: format!(
                            "Cannot update range type with new subtype. Existing subtype = '{}', New subtype = '{}'",
                            existing_subtype,
                            new_subtype
                        ),
                    });
                }
                Ok(String::new())
            }
            (_, _) => Err(PgDiffError::IncompatibleTypes {
                name: self.name.clone(),
                original_type: self.udt_type.name().into(),
                new_type: other.udt_type.name().into(),
            }),
        }
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
    collation: Option<String>,
}

impl CompositeField {
    fn field_definition(&self) -> String {
        format!(
            "{} {}{}",
            self.name,
            self.data_type,
            self.collation.as_deref().unwrap_or_default()
        )
    }
}

#[derive(Debug)]
pub struct Table {
    name: SchemaQualifiedName,
    columns: Vec<Column>,
    policies: Option<Vec<Policy>>,
    partition_key_def: Option<String>,
    partition_values: Option<String>,
    inherited_tables: Option<Vec<SchemaQualifiedName>>,
    partitioned_parent_table: Option<SchemaQualifiedName>,
    tablespace: Option<String>,
    with: Option<Vec<String>>,
}

impl<'r> FromRow<'r, PgRow> for Table {
    fn from_row(row: &'r PgRow) -> Result<Self, SqlxError> {
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let columns: Json<Vec<Column>> = row.try_get("columns")?;
        let policies: Option<Json<Vec<Policy>>> = row.try_get("policies")?;
        let partition_key_def: Option<String> = row.try_get("partition_key_def")?;
        let partition_values: Option<String> = row.try_get("partition_values")?;
        let inherited_tables: Option<Json<Vec<SchemaQualifiedName>>> =
            row.try_get("inherited_tables")?;
        let partitioned_parent_table: Option<Json<SchemaQualifiedName>> =
            row.try_get("partitioned_parent_table")?;
        let tablespace: Option<String> = row.try_get("tablespace")?;
        let with: Option<Vec<String>> = row.try_get("with")?;
        Ok(Self {
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
    fn create_statement(&self) -> String {
        let mut result = format!("CREATE TABLE {}\n", self.name);
        if let Some(partitioned_parent_table) = &self.partitioned_parent_table {
            result.push_str(" PARTITION OF ");
            result.push_str(partitioned_parent_table.to_string().as_str());
        } else if !self.columns.is_empty() {
            result.push_str("(\n\t");
            result.push_str(
                self.columns
                    .iter()
                    .map(|c| c.field_definition())
                    .join(",\n\t")
                    .as_str(),
            );
            result.push_str("\n)");
        }
        match &self.partition_values {
            Some(partition_values) => {
                result.push_str("\nFOR VALUES ");
                result.push_str(partition_values)
            }
            None if self.partitioned_parent_table.is_some() => {
                result.push_str("\nDEFAULT");
            }
            _ => {}
        }
        match &self.inherited_tables {
            Some(inherited_tables) if !inherited_tables.is_empty() => {
                result.push_str("\nINHERITS (");
                result.push_str(
                    inherited_tables
                        .iter()
                        .map(|t| t.to_string())
                        .join(",")
                        .as_str(),
                );
                result.push(')');
            }
            _ => {}
        }
        if let Some(partition_key_def) = &self.partition_key_def {
            result.push_str("\nPARTITION BY ");
            result.push_str(partition_key_def);
        }
        if let Some(storage_parameter) = &self.with {
            result.push_str("WITH (");
            for parameter in storage_parameter {
                result.push_str(parameter);
            }
            result.push(')');
        }
        if let Some(tablespace) = &self.tablespace {
            result.push_str("\nTABLESPACE ");
            result.push_str(tablespace);
        }
        result.push(';');
        result
    }

    fn alter_statement(&self, other: &Self) -> Result<String, PgDiffError> {
        todo!()
    }
}

#[derive(Debug, Deserialize)]
pub struct Column {
    name: String,
    data_type: String,
    size: i32,
    collation: Option<String>,
    is_non_null: bool,
    default_expression: Option<String>,
    generated_column: Option<GeneratedColumn>,
    identity_column: Option<IdentityColumn>,
    storage: Option<Storage>,
    compression: Compression,
}

impl Column {
    fn field_definition(&self) -> String {
        let mut result = format!("{} {}", self.name, self.data_type);
        if let Some(storage) = &self.storage {
            result.push_str(storage.as_ref());
        }
        result.push_str(self.compression.as_ref());
        if let Some(collation) = &self.collation {
            result.push_str(" COLLATE ");
            result.push_str(collation);
        }
        result.push_str(if self.is_non_null {
            " NOT NULL"
        } else {
            " NULL"
        });
        if let Some(default_expression) = &self.default_expression {
            result.push_str(" DEFAULT ");
            result.push_str(default_expression);
        }
        if let Some(generated_column) = &self.generated_column {
            result.push_str(generated_column.definition().as_str());
        }
        if let Some(identity_column) = &self.identity_column {
            result.push_str(identity_column.definition().as_str());
        }
        result
    }
}

#[derive(Debug, Deserialize)]
pub struct GeneratedColumn {
    expression: String,
    generation_type: GeneratedColumnType,
}

impl GeneratedColumn {
    fn definition(&self) -> String {
        format!(
            " GENERATED ALWAYS AS ({}) {}",
            self.expression,
            self.generation_type.as_ref()
        )
    }
}

#[derive(Debug, Deserialize, strum::AsRefStr)]
pub enum GeneratedColumnType {
    #[strum(serialize = "STORED")]
    Stored,
}

#[derive(Debug, Deserialize)]
pub struct IdentityColumn {
    identity_generation: IdentityGeneration,
    sequence_options: SequenceOptions,
}

impl IdentityColumn {
    fn definition(&self) -> String {
        format!(
            " GENERATED {} AS IDENTITY ({})",
            self.identity_generation.as_ref(),
            self.sequence_options.definition(),
        )
    }
}

#[derive(Debug, Deserialize, strum::AsRefStr)]
pub enum IdentityGeneration {
    #[strum(serialize = "ALWAYS")]
    Always,
    #[strum(serialize = "DEFAULT")]
    Default,
}

#[derive(Debug, Deserialize, strum::AsRefStr)]
pub enum Storage {
    #[serde(alias = "p")]
    #[strum(serialize = " STORAGE PLAIN")]
    Plain,
    #[serde(alias = "e")]
    #[strum(serialize = " STORAGE EXTERNAL")]
    External,
    #[serde(alias = "m")]
    #[strum(serialize = " STORAGE MAIN")]
    Main,
    #[serde(alias = "x")]
    #[strum(serialize = " STORAGE EXTENDED")]
    Extended,
}

#[derive(Debug, Deserialize, strum::AsRefStr)]
pub enum Compression {
    #[serde(alias = "")]
    #[strum(serialize = "")]
    Default,
    #[serde(alias = "p")]
    #[strum(serialize = " COMPRESSION pglz")]
    PGLZ,
    #[serde(alias = "l")]
    #[strum(serialize = " COMPRESSION lz4")]
    LZ4,
}

#[derive(Debug, Deserialize)]
pub struct Constraint {
    name: String,
    owning_table: SchemaQualifiedName,
    constraint_type: ConstraintType,
    timing: ConstraintTiming,
}

impl<'r> FromRow<'r, PgRow> for Constraint {
    fn from_row(row: &'r PgRow) -> Result<Self, SqlxError> {
        let name = row.try_get("name")?;
        let owning_table: Json<SchemaQualifiedName> = row.try_get("owning_table")?;
        let constraint_type: Json<ConstraintType> = row.try_get("constraint_type")?;
        let timing: Json<ConstraintTiming> = row.try_get("timing")?;
        Ok(Self {
            name,
            owning_table: owning_table.0,
            constraint_type: constraint_type.0,
            timing: timing.0,
        })
    }
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type")]
pub enum ConstraintTiming {
    #[default]
    NotDeferrable,
    Deferrable {
        is_immediate: bool,
    },
}

#[derive(Debug, Default, Deserialize)]
pub enum ForeignKeyMatch {
    Full,
    Partial,
    #[default]
    Simple,
}

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug)]
pub struct Index {
    name: String,
    owning_table: SchemaQualifiedName,
    columns: Vec<String>,
    is_valid: bool,
    definition_statement: String,
    parameters: IndexParameters,
}

impl<'r> FromRow<'r, PgRow> for Index {
    fn from_row(row: &'r PgRow) -> Result<Self, SqlxError> {
        let name = row.try_get("name")?;
        let owning_table: Json<SchemaQualifiedName> = row.try_get("owning_table")?;
        let columns: Vec<String> = row.try_get("columns")?;
        let is_valid: bool = row.try_get("is_valid")?;
        let definition_statement: String = row.try_get("definition_statement")?;
        Ok(Self {
            name,
            owning_table: owning_table.0,
            columns,
            is_valid,
            definition_statement,
            parameters: IndexParameters {
                include: row.try_get("include")?,
                with: row.try_get("with")?,
                tablespace: row.try_get("tablespace")?,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct IndexParameters {
    include: Option<Vec<String>>,
    with: Option<Vec<String>>,
    tablespace: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Policy {
    name: String,
    is_permissive: bool,
    applies_to: Vec<String>,
    command: PolicyCommand,
    check_expression: Option<String>,
    using_expression: Option<String>,
    columns: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub enum PolicyCommand {
    #[serde(rename = "r")]
    Select,
    #[serde(rename = "a")]
    Insert,
    #[serde(rename = "w")]
    Update,
    #[serde(rename = "d")]
    Delete,
    #[serde(rename = "*")]
    All,
}

impl PolicyCommand {
    fn as_str(&self) -> &'static str {
        match self {
            PolicyCommand::Select => "r",
            PolicyCommand::Insert => "a",
            PolicyCommand::Update => "w",
            PolicyCommand::Delete => "d",
            PolicyCommand::All => "*",
        }
    }
}

#[derive(Debug)]
pub struct Sequence {
    name: SchemaQualifiedName,
    data_type: String,
    owner: Option<SequenceOwner>,
    sequence_options: SequenceOptions,
}

impl<'r> FromRow<'r, PgRow> for Sequence {
    fn from_row(row: &'r PgRow) -> Result<Self, SqlxError> {
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

#[derive(Debug, Deserialize)]
pub struct SequenceOptions {
    increment: i64,
    min_value: i64,
    max_value: i64,
    start_value: i64,
    cache: i64,
    is_cycle: bool,
}

impl SequenceOptions {
    fn definition(&self) -> String {
        format!(
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

#[derive(Debug, Deserialize)]
pub struct SequenceOwner {
    table_name: SchemaQualifiedName,
    column_name: String,
}

#[derive(Debug)]
pub struct Function {
    name: SchemaQualifiedName,
    signature: String,
    definition: String,
    language: String,
    function_dependencies: Option<Vec<FunctionDependency>>,
}

impl<'r> FromRow<'r, PgRow> for Function {
    fn from_row(row: &'r PgRow) -> Result<Self, SqlxError> {
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let signature: String = row.try_get("signature")?;
        let definition: String = row.try_get("definition")?;
        let language: String = row.try_get("language")?;
        let function_dependencies: Option<Json<Vec<FunctionDependency>>> =
            row.try_get("function_dependencies")?;
        Ok(Self {
            name: name.0,
            signature,
            definition,
            language,
            function_dependencies: function_dependencies.map(|j| j.0),
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct FunctionDependency {
    name: SchemaQualifiedName,
    signature: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct Trigger {
    name: String,
    #[sqlx(json)]
    owning_table: SchemaQualifiedName,
    #[sqlx(json)]
    function: SchemaQualifiedName,
    function_signature: String,
    definition: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct View {
    #[sqlx(json)]
    name: SchemaQualifiedName,
    columns: Option<Vec<String>>,
    query: String,
    options: Option<Vec<String>>,
}

#[derive(Debug, Parser)]
#[command(
    version = "0.0.1",
    about = "Postgresql schema diffing and migration tool",
    long_about = None
)]
struct Args {
    #[arg(short, long)]
    connection: String,
    #[arg(short = 'p', long)]
    files_path: PathBuf,
}

async fn get_database(pool: &PgPool) -> Result<Database, PgDiffError> {
    let schemas = get_schemas(pool).await?;
    let udts = get_udts(pool, &schemas).await?;
    let tables = get_tables(pool, &schemas).await?;
    let constraints = get_constraints(pool, &schemas).await?;
    let indexes = get_indexes(pool, &schemas).await?;
    let sequences = get_sequences(pool, &schemas).await?;
    let functions = get_functions(pool, &schemas).await?;
    let views = get_views(pool, &schemas).await?;
    let triggers = get_triggers(pool, &schemas).await?;
    Ok(Database {
        schemas,
        udts,
        tables,
        constraints,
        indexes,
        sequences,
        functions,
        triggers,
        views,
        extensions: get_extensions(pool).await?,
    })
}

async fn get_schemas(pool: &PgPool) -> Result<Vec<Schema>, PgDiffError> {
    let schemas_query = include_str!("./../queries/schemas.pgsql");
    let schema_names = match query_scalar(schemas_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load schemas");
            return Err(error.into());
        }
    };
    Ok(schema_names)
}

async fn get_udts(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Udt>, PgDiffError> {
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

async fn get_tables(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Table>, PgDiffError> {
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

async fn get_constraints(
    pool: &PgPool,
    schemas: &[Schema],
) -> Result<Vec<Constraint>, PgDiffError> {
    let constraints_query = include_str!("./../queries/constraints.pgsql");
    let constraints = match query_as(constraints_query)
        .bind(schemas)
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

async fn get_indexes(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Index>, PgDiffError> {
    let index_query = include_str!("./../queries/indexes.pgsql");
    let indexes = match query_as(index_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load indexes");
            return Err(error.into());
        }
    };
    Ok(indexes)
}

async fn get_sequences(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Sequence>, PgDiffError> {
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

async fn get_functions(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Function>, PgDiffError> {
    let functions_query = include_str!("./../queries/procs.pgsql");
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

async fn get_views(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<View>, PgDiffError> {
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

async fn get_triggers(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Trigger>, PgDiffError> {
    let trigger_queries = include_str!("./../queries/triggers.pgsql");
    let triggers = match query_as(trigger_queries)
        .bind(schemas)
        .fetch_all(pool)
        .await
    {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load triggers");
            return Err(error.into());
        }
    };
    Ok(triggers)
}

async fn get_extensions(pool: &PgPool) -> Result<Vec<Extension>, PgDiffError> {
    let extensions_query = include_str!("./../queries/extensions.pgsql");
    let extensions = match query_as(extensions_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load schemas");
            return Err(error.into());
        }
    };
    Ok(extensions)
}

#[tokio::main]
async fn main() -> Result<(), PgDiffError> {
    let args = Args::parse();
    let mut connect_options = PgConnectOptions::from_str(&args.connection)?;
    if let Ok(password) = std::env::var("PGPASSWORD") {
        connect_options = connect_options.password(&password);
    }
    let pool = PgPool::connect_with(connect_options).await?;
    let database = get_database(&pool).await?;
    // println!("{database:?}");
    if let Some(table) = database
        .tables
        .iter()
        .find(|t| t.partition_key_def.is_some())
    {
        println!("{}", table.create_statement())
    }
    Ok(())
}
