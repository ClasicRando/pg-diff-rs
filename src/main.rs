use std::convert::Into;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use serde::Deserialize;
use sqlx::{Error as SqlxError, FromRow, PgPool, query_as, query_scalar, Row, Type};
use sqlx::decode::Decode;
use sqlx::postgres::{PgConnectOptions, PgRow};
use sqlx::postgres::types::Oid;
use sqlx::types::Json;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum PgDiffError {
    #[error(transparent)]
    Sql(#[from] SqlxError),
    #[error("{0}")]
    General(String),
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

#[derive(Debug, sqlx::FromRow)]
pub struct Udt {
    #[sqlx(json)]
    name: SchemaQualifiedName,
    #[sqlx(json)]
    udt_type: UdtType,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum UdtType {
    Enum { labels: Vec<String> },
    Composite { attributes: Vec<CompositeField> },
    Range { subtype: String },
}

#[derive(Debug, Deserialize)]
pub struct CompositeField {
    name: String,
    data_type: String,
    size: i32,
    collation: Option<String>,
}

#[derive(Debug)]
pub struct Table {
    name: SchemaQualifiedName,
    columns: Vec<Column>,
    policies: Option<Vec<Policy>>,
    partition_key_def: Option<String>,
    parent_table: Option<SchemaQualifiedName>,
    partition_values: Option<String>,
    tablespace: Option<String>,
    with: Option<serde_json::Value>,
}

impl<'r> FromRow<'r, PgRow> for Table {
    fn from_row(row: &'r PgRow) -> Result<Self, SqlxError> {
        let name: Json<SchemaQualifiedName> = row.try_get("name")?;
        let columns: Json<Vec<Column>> = row.try_get("columns")?;
        let policies: Option<Json<Vec<Policy>>> = row.try_get("policies")?;
        let partition_key_def: Option<String> = row.try_get("partition_key_def")?;
        let parent_table: Option<Json<SchemaQualifiedName>> = row.try_get("parent_table")?;
        let partition_values: Option<String> = row.try_get("partition_values")?;
        let tablespace: Option<String> = row.try_get("tablespace")?;
        let with: Option<serde_json::Value> = row.try_get("with")?;
        Ok(Self {
            name: name.0,
            columns: columns.0,
            policies: policies.map(|j| j.0),
            partition_key_def,
            parent_table: parent_table.map(|j| j.0),
            partition_values,
            tablespace,
            with,
        })
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
    compression: Compression
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum GeneratedColumn {
    Stored { expression: String },
}

#[derive(Debug, Deserialize)]
pub struct IdentityColumn {
    identity_generation: IdentityGeneration,
    sequence_options: SequenceOptions,
}

#[derive(Debug, Deserialize)]
pub enum IdentityGeneration {
    Always,
    Default,
}

#[derive(Debug, Deserialize)]
pub enum Storage {
    #[serde(alias = "p")]
    Plain,
    #[serde(alias = "e")]
    External,
    #[serde(alias = "m")]
    Main,
    #[serde(alias = "x")]
    Extended,
}

#[derive(Debug, Deserialize)]
pub enum Compression {
    #[serde(alias = "")]
    Default,
    #[serde(alias = "p")]
    PGLZ,
    #[serde(alias = "l")]
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
    with: Option<serde_json::Value>,
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

#[derive(Debug)]
pub struct Trigger {
    name: String,
    timing: TriggerTiming,
    event: TriggerEvent,
    owning_table: SchemaQualifiedName,
    constraint_timing: Option<ConstraintTiming>,
    old_table: Option<String>,
    new_table: Option<String>,
    when_expression: String,
    function: SchemaQualifiedName,
}

#[derive(Debug)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug)]
pub enum TriggerEvent {
    Insert,
    Update(Option<Vec<String>>),
    Delete,
    Truncate,
}

#[derive(Debug, sqlx::FromRow)]
pub struct View {
    #[sqlx(json)]
    name: SchemaQualifiedName,
    columns: Option<Vec<String>>,
    query: String,
    options: Option<serde_json::Value>,
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
    Ok(Database {
        schemas,
        udts,
        tables,
        constraints,
        indexes,
        sequences,
        functions,
        triggers: vec![],
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
    println!("{database:?}");
    Ok(())
}
