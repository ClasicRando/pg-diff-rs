use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
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

#[derive(Debug)]
pub enum Udt {
    Enum {
        name: SchemaQualifiedName,
        labels: Vec<String>,
    },
    Composite {
        name: SchemaQualifiedName,
        attributes: Vec<CompositeField>,
    },
    Range {
        subtype: String,
    },
}

#[derive(Debug)]
pub struct CompositeField {
    name: String,
    r#type: String,
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
        Ok(Self {
            name: name.0,
            columns: columns.0,
            policies: policies.map(|j| j.0),
            partition_key_def,
            parent_table: parent_table.map(|j| j.0),
            partition_values,
            tablespace,
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
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum GeneratedColumn {
    Stored {
        expression: String,
    }
}

#[derive(Debug, Deserialize)]
pub enum IdentityColumn {
    Always,
    Default,
}

#[derive(Debug, Deserialize)]
pub struct Constraint {
    name: String,
    owning_table: SchemaQualifiedName,
    constraint_type: ConstraintType,
    timing: ConstraintTiming,
}

impl <'r> FromRow<'r, PgRow> for Constraint {
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
        columns: Option<Vec<String>>
    },
    SetDefault {
        columns: Option<Vec<String>>
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

impl <'r> FromRow<'r, PgRow> for Index {
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
    data_type: SequenceType,
    increment: i64,
    min_value: i64,
    max_value: i64,
    start_value: i64,
    cache: i64,
    is_cycle: bool,
    owner: Option<SequenceOwner>,
}

#[derive(Debug, Default)]
pub enum SequenceType {
    Smallint,
    Integer,
    #[default]
    Bigint,
}

impl SequenceType {
    fn type_name(&self) -> &'static str {
        match self {
            SequenceType::Smallint => "int2",
            SequenceType::Integer => "int4",
            SequenceType::Bigint => "int8",
        }
    }
}

#[derive(Debug)]
pub struct SequenceOwner {
    table_name: SchemaQualifiedName,
    column_name: String,
}

#[derive(Debug)]
pub struct Function {
    name: SchemaQualifiedName,
    definition: String,
    language: String,
    function_dependencies: Option<Vec<SchemaQualifiedName>>,
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

#[derive(Debug)]
pub struct View {
    name: SchemaQualifiedName,
    columns: Option<Vec<String>>,
    query: String,
    check_option: ViewCheckOption,
    is_updatable: bool,
    is_insertable: bool,
}

#[derive(Debug, Default)]
pub enum ViewCheckOption {
    #[default]
    None,
    Cascaded,
    Local,
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
    let tables = get_tables(pool, &schemas).await?;
    let constraints = get_constraints(pool, &schemas).await?;
    let indexes = get_indexes(pool, &schemas).await?;
    Ok(Database {
        schemas,
        udts: vec![],
        tables,
        constraints,
        indexes,
        sequences: vec![],
        functions: vec![],
        triggers: vec![],
        views: vec![],
        extensions: get_extensions(pool).await?,
    })
}

async fn get_schemas(pool: &PgPool) -> Result<Vec<Schema>, PgDiffError> {
    let schemas_query = include_str!("./../queries/schemas.pgsql");
    let schema_names = match query_scalar(schemas_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load schemas");
            return Err(error.into())
        }
    };
    Ok(schema_names)
}

async fn get_tables(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Table>, PgDiffError> {
    let tables_query = include_str!("./../queries/tables.pgsql");
    let tables = match query_as(tables_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load tables");
            return Err(error.into())
        }
    };
    Ok(tables)
}

async fn get_constraints(pool: &PgPool, schemas: &[Schema]) -> Result<Vec<Constraint>, PgDiffError> {
    let constraints_query = include_str!("./../queries/constraints.pgsql");
    let constraints = match query_as(constraints_query).bind(schemas).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load constraints");
            return Err(error.into())
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
            return Err(error.into())
        }
    };
    Ok(indexes)
}

async fn get_extensions(pool: &PgPool) -> Result<Vec<Extension>, PgDiffError> {
    let extensions_query = include_str!("./../queries/extensions.pgsql");
    let extensions = match query_as(extensions_query).fetch_all(pool).await {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load schemas");
            return Err(error.into())
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
