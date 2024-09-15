use std::fmt::Write;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Parser, Subcommand};
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use thiserror::Error as ThisError;

use crate::object::{set_verbose_flag, Database, DatabaseMigration, SchemaQualifiedName};

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
    #[error("Function `{object_name}` uses a language `{language}` that is not supported")]
    UnsupportedFunctionLanguage {
        object_name: SchemaQualifiedName,
        language: String,
    },
    #[error("Parse error for {object_name}. {error}")]
    PgQuery {
        object_name: SchemaQualifiedName,
        error: pg_query::Error,
    },
    #[error("Parse error for file {path}. {message}")]
    FileQueryParse { path: PathBuf, message: String },
    #[error(transparent)]
    WalkDir(#[from] async_walkdir::Error),
    #[error("Could not parse all source control statements into a temp database. Remaining\n{remaining_statements:#?}")]
    SourceControlScript { remaining_statements: Vec<String> },
}

impl From<&str> for PgDiffError {
    fn from(value: &str) -> Self {
        Self::General(value.to_string())
    }
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
        w.write_str(separator)?;
        map(item, w)?;
    }
    Ok(())
}

#[derive(Debug, Parser)]
#[command(
    version = "0.0.1",
    about = "Postgresql schema diffing and migration tool",
    long_about = None
)]
struct Args {
    #[arg(short)]
    verbose: bool,
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
    set_verbose_flag(args.verbose);
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
            let database = Database::from_connection(&pool).await?;
            database.script_out(output_path).await?;
        }
        Commands::Migrate { .. } => {
            println!("Migration is currently not supported. However, you can take the planned queries from 'plan' command to get migration steps");
        }
        Commands::Plan {
            connection,
            files_path,
        } => {
            let mut connect_options = PgConnectOptions::from_str(connection)?;
            if let Ok(password) = std::env::var("PGPASSWORD") {
                connect_options = connect_options.password(&password);
            }
            let pool = PgPool::connect_with(connect_options).await?;
            let mut database_migration = DatabaseMigration::new(pool, files_path).await?;
            let migration_script = database_migration.plan_migration().await?;
            if migration_script.is_empty() {
                println!("\nNo migration needed!");
                return Ok(());
            }
            println!("{}", migration_script);
        }
    }
    Ok(())
}
