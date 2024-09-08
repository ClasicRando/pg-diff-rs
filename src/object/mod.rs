use std::fmt::{Debug, Display, Formatter, Write};
use std::sync::OnceLock;

use serde::Deserialize;

use constraint::{get_constraints, Constraint};
pub use database::{Database, DatabaseMigration};
use extension::{get_extensions, Extension};
use function::{get_functions, Function};
use index::{get_indexes, Index};
use schema::{get_schemas, Schema};
use sequence::{get_sequences, Sequence};
use table::{get_tables, Table};
use trigger::{get_triggers, Trigger};
use udt::{get_udts, Udt};
use view::{get_views, View};

use crate::PgDiffError;

mod constraint;
mod database;
mod extension;
mod function;
mod index;
mod plpgsql;
mod policy;
mod schema;
mod sequence;
mod table;
mod trigger;
mod udt;
mod view;

const BUILT_IN_NAMES: &[&str] = &[
    "text", "oid", "inet", "jsonb", "char", "uuid", "date", "trigger", "regclass", "bigint",
];

const BUILT_IN_FUNCTIONS: &[&str] = &[
    "array_agg",
    "json_object",
    "json_agg",
    "array_length",
    "pg_notify",
    "format",
];

fn write_join_iter<W, D, I>(write: &mut W, mut iter: I, separator: &str) -> Result<(), std::fmt::Error>
where
    W: Write,
    D: Display,
    I: Iterator<Item = D>,
{
    if let Some(item) = iter.next() {
        write!(write, "{item}")?;
        for item in iter {
            write.write_str(separator)?;
            write!(write, "{item}")?;
        }
    }
    Ok(())
}

#[macro_export]
macro_rules! write_join {
    ($write:ident, $items:ident, $separator:literal) => {
        $crate::object::write_join_iter($write, $items.iter(), $separator)?;
    };
    ($write:ident, $items:expr, $separator:literal) => {
        $crate::object::write_join_iter($write, $items, $separator)?;
    };
    ($write:ident, $prefix:literal, $items:ident, $separator:literal, $postfix:literal) => {
        if !$prefix.is_empty() {
            $write.write_str($prefix)?;
        };
        write_join!($write, $items, $separator);
        if !$postfix.is_empty() {
            $write.write_str($postfix)?;
        };
    };
}

static VERBOSE_FLAG: OnceLock<bool> = OnceLock::new();

pub fn set_verbose_flag(value: bool) {
    VERBOSE_FLAG.get_or_init(|| value);
}

fn is_verbose() -> bool {
    if let Some(flag) = VERBOSE_FLAG.get() {
        return *flag;
    }
    false
}

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
                write_join!(f, include, ",");
                write!(f, ")")?;
            }
            _ => {}
        }
        match &self.with {
            Some(storage_parameters) if !storage_parameters.is_empty() => {
                write!(f, " WITH(")?;
                write_join!(f, storage_parameters, ",");
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

trait SqlObject: PartialEq {
    fn name(&self) -> &SchemaQualifiedName;
    fn object_type_name(&self) -> &str;
    fn dependencies(&self) -> &[SchemaQualifiedName];
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
    fn dependencies_met(&self, completed_objects: &[SchemaQualifiedName]) -> bool {
        self.dependencies()
            .iter()
            .all(|d| completed_objects.contains(d))
    }
}

fn compare_object_groups<S, W>(
    old_objects: &[S],
    new_objects: &[S],
    writer: &mut W,
) -> Result<(), PgDiffError>
where
    S: SqlObject + Debug,
    W: Write,
{
    for existing_object in old_objects {
        match new_objects
            .iter()
            .find(|s| s.name() == existing_object.name())
        {
            Some(new_object) if existing_object != new_object => {
                existing_object.alter_statements(new_object, writer)?
            }
            None => existing_object.drop_statements(writer)?,
            _ => {}
        }
    }
    for new_object in new_objects
        .iter()
        .filter(|s| !old_objects.iter().any(|o| o.name() == s.name()))
    {
        new_object.create_statements(writer)?;
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Deserialize)]
pub struct SchemaQualifiedName {
    pub(crate) schema_name: String,
    pub(crate) local_name: String,
}

impl<S> From<S> for SchemaQualifiedName
where
    S: AsRef<str>,
{
    fn from(value: S) -> Self {
        match value.as_ref().split_once('.') {
            Some((schema_name, local_name)) => SchemaQualifiedName {
                schema_name: schema_name.to_owned(),
                local_name: local_name.to_owned(),
            },
            None => SchemaQualifiedName {
                schema_name: "".to_string(),
                local_name: value.as_ref().to_owned(),
            },
        }
    }
}

impl SchemaQualifiedName {
    fn new(schema_name: &str, local_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_owned(),
            local_name: local_name.to_owned(),
        }
    }

    fn from_schema_name(schema_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            local_name: "".to_string(),
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

struct TablespaceCompare<'a> {
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

trait OptionListObject: SqlObject {
    fn write_alter_prefix<W>(&self, w: &mut W) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        write!(w, "ALTER {} {}", self.object_type_name(), self.name())?;
        Ok(())
    }
}

fn compare_option_lists<A, O, W>(
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

#[derive(Debug, PartialEq, Deserialize, Copy, Clone)]
enum PgCatalog {
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

#[derive(Debug, sqlx::FromRow)]
struct GenericObject {
    #[sqlx(json)]
    name: SchemaQualifiedName,
}

fn find_index<T, F>(slice: &[T], predicate: F) -> Option<usize>
where
    F: Fn(&T) -> bool,
{
    slice
        .iter()
        .enumerate()
        .filter_map(|(i, item)| if predicate(item) { Some(i) } else { None })
        .next()
}
