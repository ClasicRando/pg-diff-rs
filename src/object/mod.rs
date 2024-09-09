use std::fmt::{Debug, Display, Formatter, Write};
use std::sync::OnceLock;

use serde::Deserialize;

use constraint::{get_constraints, Constraint};
pub use database::{Database, DatabaseMigration};
use extension::{get_extensions, Extension};
use function::{get_functions, Function};
use index::{get_indexes, Index};
use policy::{get_policies, Policy};
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

fn write_join_iter<W, D, I>(
    write: &mut W,
    mut iter: I,
    separator: &str,
) -> Result<(), std::fmt::Error>
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

pub enum SqlObjectEnum<'o> {
    Schema(&'o Schema),
    Extension(&'o Extension),
    Udt(&'o Udt),
    Table(&'o Table),
    Policy(&'o Policy),
    Constraint(&'o Constraint),
    Index(&'o Index),
    Trigger(&'o Trigger),
    Sequence(&'o Sequence),
    Function(&'o Function),
    View(&'o View),
}

impl<'o> SqlObjectEnum<'o> {
    fn name(&self) -> &'o SchemaQualifiedName {
        match self {
            Self::Schema(schema) => &schema.name,
            Self::Extension(extension) => &extension.name,
            Self::Udt(udt) => &udt.name,
            Self::Table(table) => &table.name,
            Self::Policy(policy) => &policy.schema_qualified_name,
            Self::Constraint(constraint) => &constraint.schema_qualified_name,
            Self::Index(index) => &index.schema_qualified_name,
            Self::Trigger(trigger) => &trigger.schema_qualified_name,
            Self::Sequence(sequence) => &sequence.name,
            Self::Function(function) => &function.name,
            Self::View(view) => &view.name,
        }
    }

    fn object_type_name(&self) -> &str {
        match self {
            Self::Schema(schema) => schema.object_type_name(),
            Self::Extension(extension) => extension.object_type_name(),
            Self::Udt(udt) => udt.object_type_name(),
            Self::Table(table) => table.object_type_name(),
            Self::Policy(policy) => policy.object_type_name(),
            Self::Constraint(constraint) => constraint.object_type_name(),
            Self::Index(index) => index.object_type_name(),
            Self::Trigger(trigger) => trigger.object_type_name(),
            Self::Sequence(sequence) => sequence.object_type_name(),
            Self::Function(function) => function.object_type_name(),
            Self::View(view) => view.object_type_name(),
        }
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        match self {
            Self::Schema(schema) => schema.dependencies(),
            Self::Extension(extension) => extension.dependencies(),
            Self::Udt(udt) => udt.dependencies(),
            Self::Table(table) => table.dependencies(),
            Self::Policy(policy) => policy.dependencies(),
            Self::Constraint(constraint) => constraint.dependencies(),
            Self::Index(index) => index.dependencies(),
            Self::Trigger(trigger) => trigger.dependencies(),
            Self::Sequence(sequence) => sequence.dependencies(),
            Self::Function(function) => function.dependencies(),
            Self::View(view) => view.dependencies(),
        }
    }

    /// Create the `CREATE` statement for this object
    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        match self {
            Self::Schema(schema) => schema.create_statements(w),
            Self::Extension(extension) => extension.create_statements(w),
            Self::Udt(udt) => udt.create_statements(w),
            Self::Table(table) => table.create_statements(w),
            Self::Policy(policy) => policy.create_statements(w),
            Self::Constraint(constraint) => constraint.create_statements(w),
            Self::Index(index) => index.create_statements(w),
            Self::Trigger(trigger) => trigger.create_statements(w),
            Self::Sequence(sequence) => sequence.create_statements(w),
            Self::Function(function) => function.create_statements(w),
            Self::View(view) => view.create_statements(w),
        }
    }

    /// Create the `ALTER` statement(s) required for this SQL object to be migrated to the new state
    /// provided.
    ///
    /// ## Errors
    /// If the migration is not possible either due to an unsupported, impossible or invalid
    /// migration.  
    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        match (self, new) {
            (Self::Schema(old), Self::Schema(new)) if old != new => old.alter_statements(new, w),
            (Self::Extension(old), Self::Extension(new)) if old != new => {
                old.alter_statements(new, w)
            }
            (Self::Udt(old), Self::Udt(new)) if old != new => old.alter_statements(new, w),
            (Self::Table(old), Self::Table(new)) if old != new => old.alter_statements(new, w),
            (Self::Policy(old), Self::Policy(new)) if old != new => old.alter_statements(new, w),
            (Self::Constraint(old), Self::Constraint(new)) if old != new => {
                old.alter_statements(new, w)
            }
            (Self::Index(old), Self::Index(new)) if old != new => old.alter_statements(new, w),
            (Self::Trigger(old), Self::Trigger(new)) if old != new => old.alter_statements(new, w),
            (Self::Sequence(old), Self::Sequence(new)) if old != new => old.alter_statements(new, w),
            (Self::Function(old), Self::Function(new)) if old != new => old.alter_statements(new, w),
            (Self::View(old), Self::View(new)) if old != new => old.alter_statements(new, w),
            _ => Ok(()),
        }
    }

    /// Create the `DROP` statement for this object
    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        match self {
            Self::Schema(schema) => schema.drop_statements(w),
            Self::Extension(extension) => extension.drop_statements(w),
            Self::Udt(udt) => udt.drop_statements(w),
            Self::Table(table) => table.drop_statements(w),
            Self::Policy(policy) => policy.drop_statements(w),
            Self::Constraint(constraint) => constraint.drop_statements(w),
            Self::Index(index) => index.drop_statements(w),
            Self::Trigger(trigger) => trigger.drop_statements(w),
            Self::Sequence(sequence) => sequence.drop_statements(w),
            Self::Function(function) => function.drop_statements(w),
            Self::View(view) => view.drop_statements(w),
        }
    }

    fn dependencies_met(&self, completed_objects: &[SchemaQualifiedName]) -> bool {
        self.dependencies()
            .iter()
            .all(|d| completed_objects.contains(d))
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
    fn dependencies_met(&self, completed_objects: &[&SchemaQualifiedName]) -> bool {
        self.dependencies()
            .iter()
            .all(|d| completed_objects.contains(&d))
    }
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
