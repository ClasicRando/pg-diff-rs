use std::fmt::{Display, Write};
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Parser, Subcommand};
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use thiserror::Error as ThisError;

use crate::object::{
    append_create_statements_to_owner_table_file, get_database, write_create_statements_to_file,
    SchemaQualifiedName, SqlObject,
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
                for policy in database
                    .policies
                    .iter()
                    .filter(|c| c.table_oid == table.oid)
                {
                    append_create_statements_to_owner_table_file(
                        policy,
                        &policy.owner_table_name,
                        &output_path,
                    )
                        .await?
                }
                for constraint in database
                    .constraints
                    .iter()
                    .filter(|c| c.table_oid == table.oid)
                {
                    append_create_statements_to_owner_table_file(
                        constraint,
                        &constraint.owner_table_name,
                        &output_path,
                    )
                    .await?
                }
                for index in database.indexes.iter().filter(|i| i.table_oid == table.oid) {
                    append_create_statements_to_owner_table_file(
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
                    append_create_statements_to_owner_table_file(
                        trigger,
                        &trigger.owner_table_name,
                        &output_path,
                    )
                    .await?
                }
            }
            for sequence in &database.sequences {
                if let Some(owner_table) = &sequence.owner {
                    append_create_statements_to_owner_table_file(
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
