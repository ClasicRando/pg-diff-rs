use std::fmt::Write;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Parser, Subcommand};
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use thiserror::Error as ThisError;

use crate::object::{Database, DatabaseMigration, SchemaQualifiedName};

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
    SourceControlScript {
        remaining_statements: Vec<String>
    }
}

#[macro_export]
macro_rules! write_join {
    ($write:ident, $items:ident.iter(), $separator:literal) => {
        let mut iter = $items.iter();
        if let Some(item) = iter.next() {
            write!($write, "{item}")?;
            for item in iter {
                $write.write_str($separator)?;
                write!($write, "{item}")?;
            }
        };
    };
    ($write:ident, $items:expr, $separator:literal) => {
        let mut iter = $items;
        if let Some(item) = iter.next() {
            write!($write, "{item}")?;
            for item in iter {
                $write.write_str($separator)?;
                write!($write, "{item}")?;
            }
        };
    };
    ($write:ident, $prefix:literal, $items:expr, $separator:literal, $postfix:literal) => {
        if !$prefix.is_empty() {
            $write.write_str($prefix)?;
        };
        let mut iter = $items;
        if let Some(item) = iter.next() {
            write!($write, "{item}")?;
            for item in iter {
                $write.write_str($separator)?;
                write!($write, "{item}")?;
            }
        };
        if !$postfix.is_empty() {
            $write.write_str($postfix)?;
        };
    };
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
            let database = Database::from_connection(pool).await?;
            database.script_out(output_path).await?;
        }
        Commands::Migrate { .. } => {}
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
            println!("{}", migration_script);
        }
    }
    Ok(())
}
