use std::fmt::{Display, Formatter, Write};
use std::path::Path;

use serde::Deserialize;
use sqlx::PgPool;
use sqlx::postgres::types::Oid;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub use constraint::{Constraint, get_constraints};
pub use extension::{Extension, get_extensions};
pub use function::{Function, get_functions};
pub use index::{get_indexes, Index};
pub use schema::{get_schemas, Schema};
pub use sequence::{get_sequences, Sequence};
pub use table::{get_tables, Table};
pub use trigger::{get_triggers, Trigger};
pub use udt::{get_udts, Udt};
pub use view::{get_views, View};

use crate::{PgDiffError, write_join};
use crate::object::policy::{get_policies, Policy};

mod constraint;
mod extension;
mod function;
mod index;
mod policy;
mod schema;
mod sequence;
mod table;
mod trigger;
mod udt;
mod view;
mod plpgsql;

#[derive(Debug, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct StorageParameter(pub(crate) String);

impl Display for StorageParameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, PartialEq, Deserialize, sqlx::FromRow)]
pub struct IndexParameters {
    pub(crate) include: Option<Vec<String>>,
    pub(crate) with: Option<Vec<StorageParameter>>,
    pub(crate) tablespace: Option<TableSpace>,
}

impl Display for IndexParameters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.include {
            Some(include) if !include.is_empty() => {
                write!(f, " INCLUDE(")?;
                write_join!(f, include.iter(), ",");
                write!(f, ")")?;
            }
            _ => {}
        }
        match &self.with {
            Some(storage_parameters) if !storage_parameters.is_empty() => {
                write!(f, " WITH(")?;
                write_join!(f, storage_parameters.iter(), ",");
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

pub trait SqlObject: PartialEq {
    fn name(&self) -> &SchemaQualifiedName;
    fn object_type_name(&self) -> &str;
    fn dependency_declaration(&self) -> Dependency;
    fn dependencies(&self) -> &[Dependency];
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
    fn dependencies_met(&self, completed_objects: &[Dependency]) -> bool {
        self.dependencies()
            .iter()
            .all(|d| completed_objects.contains(d))
    }
}

#[derive(Debug)]
pub struct CustomType {
    pub(crate) oid: Oid,
    pub(crate) name: String,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Deserialize)]
pub struct SchemaQualifiedName {
    pub(crate) schema_name: String,
    pub(crate) local_name: String,
}

impl SchemaQualifiedName {
    fn from_schema_name(schema_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            local_name: "".to_string(),
        }
    }

    fn from_type_name(schema_qualified_name: &str) -> Self {
        let parts = schema_qualified_name.split_once(".").unwrap();
        Self {
            schema_name: parts.0.to_string(),
            local_name: parts.1.to_string(),
        }
    }
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
pub struct Collation(pub(crate) String);

impl Display for Collation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "COLLATE {}", self.0)
    }
}

impl Collation {
    pub fn is_default(&self) -> bool {
        self.0.as_str() == "\"pg_catalog\".\"default\""
    }
}

#[derive(Debug, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct TableSpace(pub(crate) String);

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
    pub fn new(old: Option<&'a TableSpace>, new: Option<&'a TableSpace>) -> Self {
        Self { old, new }
    }

    pub fn has_diff(&self) -> bool {
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

pub trait OptionListObject: SqlObject {
    fn write_alter_prefix<W>(&self, w: &mut W) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        write!(w, "ALTER {} {}", self.object_type_name(), self.name())?;
        Ok(())
    }
}

pub fn compare_option_lists<A, O, W>(
    object: &A,
    old: Option<&[O]>,
    new: Option<&[O]>,
    w: &mut W,
) -> Result<(), PgDiffError>
where
    A: OptionListObject,
    O: Display + PartialEq,
    W: Write,
{
    if let Some(new_options) = new {
        let old_options = old.unwrap_or_default();
        object.write_alter_prefix(w)?;
        w.write_str("SET (")?;
        write_join!(
            w,
            new_options.iter().filter(|p| old_options.contains(*p)),
            ","
        );
        w.write_str(");\n")?;
    }
    if let Some(old_options) = old {
        let new_options = new.unwrap_or_default();
        object.write_alter_prefix(w)?;
        w.write_str("RESET (")?;
        for p in old_options.iter().filter(|p| new_options.contains(*p)) {
            let option = p.to_string();
            if let Some((first, _)) = option.split_once('=') {
                w.write_str(first)?;
            } else {
                w.write_str(option.as_str())?;
            }
        }
        w.write_str(");\n")?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Database {
    pub(crate) schemas: Vec<Schema>,
    pub(crate) udts: Vec<Udt>,
    pub(crate) tables: Vec<Table>,
    pub(crate) policies: Vec<Policy>,
    pub(crate) constraints: Vec<Constraint>,
    pub(crate) indexes: Vec<Index>,
    pub(crate) triggers: Vec<Trigger>,
    pub(crate) sequences: Vec<Sequence>,
    pub(crate) functions: Vec<Function>,
    pub(crate) views: Vec<View>,
    pub(crate) extensions: Vec<Extension>,
}

pub async fn get_database(pool: &PgPool) -> Result<Database, PgDiffError> {
    let mut schemas = get_schemas(pool).await?;
    let schema_names: Vec<&str> = schemas
        .iter()
        .map(|s| s.name.schema_name.as_str())
        .collect();
    let udts = get_udts(pool, &schema_names).await?;
    let tables = get_tables(pool, &schema_names).await?;
    let table_oids: Vec<Oid> = tables.iter().map(|t| t.oid).collect();
    let policies = get_policies(pool, &table_oids).await?;
    let constraints = get_constraints(pool, &table_oids).await?;
    let indexes = get_indexes(pool, &table_oids).await?;
    let triggers = get_triggers(pool, &table_oids).await?;
    let sequences = get_sequences(pool, &schema_names).await?;
    let functions = get_functions(pool, &schema_names).await?;
    let views = get_views(pool, &schema_names).await?;
    if let Some(index) = schemas
        .iter()
        .enumerate()
        .find(|(_, schema)| schema.name.schema_name == "public")
        .map(|(i, _)| i)
    {
        schemas.remove(index);
    }
    Ok(Database {
        schemas,
        udts,
        tables,
        policies,
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
pub async fn write_create_statements_to_file<S, P>(
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

pub async fn append_create_statements_to_owner_table_file<S, P>(
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

#[derive(Debug, PartialEq, Deserialize)]
pub enum PgCatalog {
    #[serde(rename = "pg_namespace")]
    Namespace,
    #[serde(rename = "pg_proc")]
    Proc,
    #[serde(rename = "pg_class")]
    Class,
    #[serde(rename = "pg_type")]
    Type,
    #[serde(rename = "pg_constraint")]
    Constraint,
    #[serde(rename = "pg_trigger")]
    Trigger,
    #[serde(rename = "pg_policy")]
    Policy,
    #[serde(rename = "pg_extension")]
    Extension,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Dependency {
    oid: Oid,
    catalog: PgCatalog,
}
