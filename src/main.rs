use std::fmt::{Display, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{Parser, Subcommand};
use sqlx::postgres::types::Oid;
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use thiserror::Error as ThisError;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::object::{
    get_constraints, get_extensions, get_functions, get_indexes, get_schemas, get_sequences,
    get_tables, get_triggers, get_udts, get_views, Constraint, Extension, Function, Index, Schema,
    SchemaQualifiedName, Sequence, SqlObject, Table, Trigger, Udt, View,
};

mod object;

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

pub async fn get_database(pool: &PgPool) -> Result<Database, PgDiffError> {
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
                    append_create_statements_table_to_file(
                        constraint,
                        &constraint.owner_table_name,
                        &output_path,
                    )
                    .await?
                }
                for index in database.indexes.iter().filter(|i| i.table_oid == table.oid) {
                    append_create_statements_table_to_file(
                        index,
                        &index.owner_table_name,
                        &output_path,
                    )
                    .await?
                }
                for trigger in database
                    .triggers
                    .iter()
                    .filter(|t| t.table_oid == table.oid)
                {
                    append_create_statements_table_to_file(
                        trigger,
                        &trigger.owner_table_name,
                        &output_path,
                    )
                    .await?
                }
            }
            for sequence in &database.sequences {
                if let Some(owner_table) = &sequence.owner {
                    append_create_statements_table_to_file(
                        sequence,
                        &owner_table.table_name,
                        &output_path,
                    )
                    .await?;
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
