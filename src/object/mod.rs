use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter, Write};
use std::ops::Deref;
use std::sync::OnceLock;

use serde::Deserialize;
use sqlx::error::BoxDynError;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{PgTypeInfo, PgValueRef};
use sqlx::{query_scalar, PgPool, Postgres};

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

fn write_join_map<W, T, I, F>(
    write: &mut W,
    mut iter: I,
    writer: F,
    separator: &str,
) -> Result<(), std::fmt::Error>
where
    W: Write,
    I: Iterator<Item = T>,
    F: Fn(&mut W, T) -> Result<(), std::fmt::Error>,
{
    if let Some(item) = iter.next() {
        writer(write, item)?;
        for item in iter {
            write.write_str(separator)?;
            writer(write, item)?;
        }
    }
    Ok(())
}

/// Join the items of an iterator by writing there contents to an object of type [W] separated by
/// the [separator] characters specified.
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

/// Write iterable types to a specified writable object. This macro wraps the [write_join_iter]
/// function but allows for iterator expression to be supplied as well as prefix and suffix values
/// to be specified.
#[macro_export]
macro_rules! write_join {
    ($write:ident, $items:ident, $separator:literal) => {
        $crate::object::write_join_iter($write, $items.iter(), $separator)?;
    };
    ($write:ident, $items:expr, $separator:literal) => {
        $crate::object::write_join_iter($write, $items, $separator)?;
    };
    ($write:ident, $items:ident, $mapper:expr, $separator:literal) => {
        $crate::object::write_join_map($write, $items.iter(), $mapper, $separator)?;
    };
    ($write:ident, $items:expr, $mapper:expr, $separator:literal) => {
        $crate::object::write_join_map($write, $items, $mapper, $separator)?;
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
    ($write:ident, $prefix:literal, $items:expr, $separator:literal, $postfix:literal) => {
        if !$prefix.is_empty() {
            $write.write_str($prefix)?;
        };
        write_join!($write, $items, $separator);
        if !$postfix.is_empty() {
            $write.write_str($postfix)?;
        };
    };
    ($write:ident, $prefix:literal, $items:ident, $mapper:expr, $separator:literal, $postfix:literal) => {
        if !$prefix.is_empty() {
            $write.write_str($prefix)?;
        };
        write_join!($write, $items, $mapper, $separator);
        if !$postfix.is_empty() {
            $write.write_str($postfix)?;
        };
    };
    ($write:ident, $prefix:literal, $items:expr, $mapper:expr, $separator:literal, $postfix:literal) => {
        if !$prefix.is_empty() {
            $write.write_str($prefix)?;
        };
        write_join!($write, $items, $mapper, $separator);
        if !$postfix.is_empty() {
            $write.write_str($postfix)?;
        };
    };
}

/// Static state of the verbose option within the application. DO NOT ACCESS directly but rather
/// use the [set_verbose_flag] and [is_verbose] functions.
static VERBOSE_FLAG: OnceLock<bool> = OnceLock::new();

/// Initialize the [VERBOSE_FLAG] option if not already set. If already set, then this function
/// does nothing.
pub fn set_verbose_flag(value: bool) {
    VERBOSE_FLAG.get_or_init(|| value);
}

/// Get the state of the [VERBOSE_FLAG] option. If the value cannot be obtained, false is returned
fn is_verbose() -> bool {
    if let Some(flag) = VERBOSE_FLAG.get() {
        return *flag;
    }
    false
}

/// Storage parameters for data objects persisted within a database (i.e. tables and indexes).
/// Although this is a string, the underlining value is a key value pair separated by an `=`.
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct KeyValuePairs(HashMap<String, String>);

impl<S> From<&[S]> for KeyValuePairs
where
    S: AsRef<str>,
{
    fn from(value: &[S]) -> Self {
        Self(
            value
                .iter()
                .map(|kvp| {
                    let (key, value) = kvp.as_ref().split_once('=').unwrap();
                    (key.to_string(), value.to_string())
                })
                .collect(),
        )
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for KeyValuePairs {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let str = <Vec<String> as sqlx::Decode<Postgres>>::decode(value)?;
        let with: HashMap<String, String> = str
            .iter()
            .map(|p| {
                let Some((first, second)) = p.split_once('=') else {
                    return (p.to_string(), String::new());
                };
                (first.to_string(), second.to_string())
            })
            .collect();
        Ok(KeyValuePairs(with))
    }
}

impl sqlx::Type<Postgres> for KeyValuePairs {
    fn type_info() -> PgTypeInfo {
        // text
        PgTypeInfo::with_oid(Oid(25))
    }
}

impl Deref for KeyValuePairs {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Macro to implement [sqlx::Type] and [sqlx::Decode] for the specified type
#[macro_export]
macro_rules! impl_type_for_kvp_wrapper {
    ($e:ident) => {
        impl sqlx::Type<sqlx::Postgres> for $e {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                KeyValuePairs::type_info()
            }
        }

        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $e {
            fn decode(
                value: sqlx::postgres::PgValueRef<'r>,
            ) -> Result<Self, sqlx::error::BoxDynError> {
                let kvp = <KeyValuePairs as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
                Ok($e(kvp))
            }
        }

        impl<S> From<&[S]> for $e
        where
            S: AsRef<str>,
        {
            fn from(value: &[S]) -> Self {
                Self(KeyValuePairs::from(value))
            }
        }

        impl std::ops::Deref for $e {
            type Target = KeyValuePairs;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

/// Storage parameters for data objects persisted within a database (i.e. tables and indexes).
/// Although this is a string, the underlining value is a key value pair separated by an `=`.
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct StorageParameters(KeyValuePairs);

impl_type_for_kvp_wrapper!(StorageParameters);

impl Display for StorageParameters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        f.write_str("WITH(")?;
        for (key, value) in self.0.iter() {
            write!(f, "{key}={value}")?;
        }
        f.write_char(')')
    }
}

/// Options that can be specified by a table index
#[derive(Debug, PartialEq, Deserialize, sqlx::FromRow, Clone)]
pub struct IndexParameters {
    /// Optional list of columns included in an index
    pub(crate) include: Option<Vec<String>>,
    /// Optional map of storage parameters for the index
    pub(crate) with: Option<StorageParameters>,
    /// Optional tablespace specified for the index. [None] means the default tablespace is used
    pub(crate) tablespace: Option<TableSpace>,
}

impl Display for IndexParameters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.include {
            Some(include) if !include.is_empty() => {
                write!(f, " INCLUDE(")?;
                write_join!(f, include, ",");
                write!(f, ")")?;
            },
            _ => {},
        }
        if let Some(storage_parameters) = &self.with {
            write!(f, "{storage_parameters}")?
        }
        if let Some(tablespace) = &self.tablespace {
            write!(f, " USING INDEX TABLESPACE {}", tablespace)?;
        }
        Ok(())
    }
}

/// Union type of the varying SQL object types. This is used to allow returning of a generic SQL
/// object during iteration because the [SqlObject] trait is not object safe. To reduce the size
/// of the enum of not copy data, all items are references to their respective [SqlObject].
#[derive(Debug)]
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

#[allow(dead_code)]
impl<'o> SqlObjectEnum<'o> {
    /// Calls the trait method [SqlObject::name] of each variant
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

    /// Calls the trait method [SqlObject::object_type_name] of each variant
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

    /// Calls the trait method [SqlObject::dependencies] of each variant
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

    /// Calls the trait method [SqlObject::create_statements] of each variant
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

    /// Calls the trait method [SqlObject::alter_statements] of each variant
    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        match (self, new) {
            (Self::Schema(old), Self::Schema(new)) if old != new => old.alter_statements(new, w),
            (Self::Extension(old), Self::Extension(new)) if old != new => {
                old.alter_statements(new, w)
            },
            (Self::Udt(old), Self::Udt(new)) if old != new => old.alter_statements(new, w),
            (Self::Table(old), Self::Table(new)) if old != new => old.alter_statements(new, w),
            (Self::Policy(old), Self::Policy(new)) if old != new => old.alter_statements(new, w),
            (Self::Constraint(old), Self::Constraint(new)) if old != new => {
                old.alter_statements(new, w)
            },
            (Self::Index(old), Self::Index(new)) if old != new => old.alter_statements(new, w),
            (Self::Trigger(old), Self::Trigger(new)) if old != new => old.alter_statements(new, w),
            (Self::Sequence(old), Self::Sequence(new)) if old != new => {
                old.alter_statements(new, w)
            },
            (Self::Function(old), Self::Function(new)) if old != new => {
                old.alter_statements(new, w)
            },
            (Self::View(old), Self::View(new)) if old != new => old.alter_statements(new, w),
            _ => Ok(()),
        }
    }

    /// Calls the trait method [SqlObject::drop_statements] of each variant
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

    /// Calls the trait method [SqlObject::dependencies_met] of each variant
    fn dependencies_met(&self, completed_objects: &[SchemaQualifiedName]) -> bool {
        self.dependencies()
            .iter()
            .all(|d| completed_objects.contains(d))
    }
}

trait SqlObject: PartialEq {
    /// Unique schema qualified name for the object within the database
    fn name(&self) -> &SchemaQualifiedName;
    /// General object type name used for creating generic SQL statements
    fn object_type_name(&self) -> &str;
    /// Declared dependencies of this object as a slice of [SchemaQualifiedName]s
    fn dependencies(&self) -> &[SchemaQualifiedName];
    /// Create the `CREATE` statement for this object
    ///
    /// ## Errors
    /// If a drop statement cannot be derived or a formatting error occurs
    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError>;
    /// Create the `ALTER` statement(s) required for this SQL object to be migrated to the new state
    /// provided.
    ///
    /// ## Errors
    /// If the migration is not possible either due to an unsupported, impossible or invalid
    /// migration. Can also fail when a formatting error occurs.
    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError>;
    /// Create the `DROP` statement for this object.
    ///
    /// ## Errors
    /// If a drop statement cannot be derived or a formatting error occurs
    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError>;
    /// Returns true if all dependencies of this object have been resolved based upon the list of
    /// `completed_objects` provided.
    fn dependencies_met(&self, completed_objects: &[&SchemaQualifiedName]) -> bool {
        self.dependencies()
            .iter()
            .filter(|d| !d.is_implicit_schema())
            .all(|d| completed_objects.contains(&d))
    }
    /// Write the beginning of an `ALTER` statement based upon the object's
    /// [SqlObject::object_type_name] and [SqlObject::name]. This can be overridden if the object
    /// requires a more complex `ALTER` statement beginning
    fn write_alter_prefix<W>(&self, w: &mut W) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        write!(w, "ALTER {} {}", self.object_type_name(), self.name())?;
        Ok(())
    }
}

/// Database unique name as the combination of the object's owning schema and the name within the
/// schema. However, not every case needs both values to be non-empty value. The major exceptions
/// are:
/// - schema objects which only have a `schema_name` and `local_name` is empty
/// - extension objects which only have a  `local_name` since extensions are not always linked to a
///     schema
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone, Deserialize)]
pub struct SchemaQualifiedName {
    /// Schema name that owned the object. Can be empty if extension object
    pub(crate) schema_name: String,
    /// Local name within the parent namespace. Can be empty if the object is a schema. This can
    /// also include a '.' for objects that are implicitly owned by another object. For instance,
    /// constraints only exist within the scope of a table so the `local_name` would be
    /// 'table_name.constraint_name'.
    pub(crate) local_name: String,
}

impl<'r> sqlx::Decode<'r, Postgres> for SchemaQualifiedName {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let json = sqlx::types::Json::decode(value)?;
        Ok(json.0)
    }
}

impl sqlx::Type<Postgres> for SchemaQualifiedName {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("jsonb")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        *ty == PgTypeInfo::with_name("json")
    }
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
    /// Create a new [SchemaQualifiedName] instance from the direct schema + local parts. Only use
    /// this method if the components are known ahead of time. If you need to split an already
    /// qualified name, then use the [From] trait implementation for this type.
    fn new(schema_name: &str, local_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_owned(),
            local_name: local_name.to_owned(),
        }
    }

    /// Returns true if the qualified name is the `public` or `pg_catalog` schemas
    fn is_implicit_schema(&self) -> bool {
        if !self.local_name.is_empty() {
            return false;
        }
        self.schema_name == PUBLIC_SCHEMA_NAME || self.schema_name == PG_CATALOG_SCHEMA_NAME
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

/// Collation name wrapper type
#[derive(Debug, PartialEq, Deserialize, sqlx::Type)]
#[sqlx(transparent)]
pub struct Collation(pub(crate) String);

impl Display for Collation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "COLLATE {}", self.0)
    }
}

impl Collation {
    /// Returns true if this is the default collation (i.e. "pg_catalog"."default")
    pub fn is_default(&self) -> bool {
        self.0.as_str() == "\"pg_catalog\".\"default\""
    }
}

/// Wrapper type for a tablespace name
#[derive(Debug, Deserialize, PartialEq, sqlx::Type, Clone)]
#[sqlx(transparent)]
pub struct TableSpace(pub(crate) String);

impl Display for TableSpace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Compare the tablespace option of 2 objects. Writes the `SET TABLESPACE` command based on the 2
/// states of the tablespace option.
fn compare_tablespaces<S, W>(
    object: &S,
    old: Option<&TableSpace>,
    new: Option<&TableSpace>,
    w: &mut W,
) -> Result<(), PgDiffError>
where
    S: SqlObject,
    W: Write,
{
    match (old, new) {
        (Some(old_tablespace), Some(new_tablespace)) if old_tablespace != new_tablespace => {
            object.write_alter_prefix(w)?;
            write!(w, " SET TABLESPACE {new_tablespace};")?;
        },
        (Some(_), None) => {
            object.write_alter_prefix(w)?;
            write!(w, " SET TABLESPACE pg_default;")?;
        },
        (None, Some(new_tablespace)) => {
            object.write_alter_prefix(w)?;
            write!(w, " SET TABLESPACE {new_tablespace};")?;
        },
        _ => {},
    }
    Ok(())
}

/// Compare the old and new versions of an object's option list and write the required `SET`/`RESET`
/// statements for the object.
fn compare_key_value_pairs<A, K, W>(
    w: &mut W,
    object: &A,
    old: &Option<K>,
    new: &Option<K>,
    within_brackets: bool,
) -> Result<(), PgDiffError>
where
    A: SqlObject,
    K: Deref<Target = KeyValuePairs>,
    W: Write,
{
    match (
        old.as_deref().map(|o| o.deref()),
        new.as_deref().map(|n| n.deref()),
    ) {
        (Some(old_options), Some(new_options)) => {
            set_key_value_pairs(
                w,
                object,
                new_options.iter().filter(|(key, value)| {
                    if let Some(old) = old_options.get(*key) {
                        return old != *value;
                    }
                    true
                }),
                within_brackets,
            )?;
            reset_key_value_pairs(
                w,
                object,
                old_options
                    .iter()
                    .filter(|(key, _)| !new_options.contains_key(*key)),
                within_brackets,
            )?;
        },
        (_, Some(new_options)) if !new_options.is_empty() => {
            set_key_value_pairs(w, object, new_options.iter(), within_brackets)?;
        },
        (Some(old_options), _) if !old_options.is_empty() => {
            reset_key_value_pairs(w, object, old_options.iter(), within_brackets)?;
        },
        _ => {},
    };
    Ok(())
}

fn set_key_value_pairs<'a, W, A, I>(
    w: &'a mut W,
    object: &'a A,
    set_options: I,
    within_brackets: bool,
) -> Result<(), PgDiffError>
where
    W: Write,
    A: SqlObject,
    I: Iterator<Item = (&'a String, &'a String)>,
{
    let mut set_options: Vec<_> = set_options.collect();
    if set_options.is_empty() {
        return Ok(());
    }

    set_options.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    object.write_alter_prefix(w)?;
    if within_brackets {
        write_join!(
            w,
            " SET (",
            set_options,
            |write, (key, value)| write!(write, "{key}={value}"),
            ",",
            ");\n"
        );
    } else {
        write_join!(
            w,
            "\nSET ",
            set_options,
            |write, (key, value)| write!(write, "{key}={value}"),
            "\nSET",
            ";\n"
        );
    }
    Ok(())
}

fn reset_key_value_pairs<'a, W, A, I>(
    w: &'a mut W,
    object: &'a A,
    reset_options: I,
    within_brackets: bool,
) -> Result<(), PgDiffError>
where
    W: Write,
    A: SqlObject,
    I: Iterator<Item = (&'a String, &'a String)>,
{
    let mut reset_options: Vec<_> = reset_options.collect();
    if reset_options.is_empty() {
        return Ok(());
    }

    reset_options.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    object.write_alter_prefix(w)?;
    if within_brackets {
        write_join!(
            w,
            " RESET (",
            reset_options,
            |write, (key, _)| write!(write, "{key}"),
            ",",
            ");\n"
        );
    } else {
        write_join!(
            w,
            "\nRESET ",
            reset_options,
            |write, (key, _)| write!(write, "{key}"),
            "\nRESET ",
            ";\n"
        );
    }
    Ok(())
}

/// Find the index of the first element found within the `slice` using the `predicate` as an element
/// selector. Returns [None] if no element is found that matches the `predicate`.
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

const PUBLIC_SCHEMA_NAME: &str = "public";
const PG_CATALOG_SCHEMA_NAME: &str = "pg_catalog";

async fn check_names_in_database(
    pool: &PgPool,
    schema_qualified_name: &SchemaQualifiedName,
    query: &str,
) -> Result<Vec<SchemaQualifiedName>, sqlx::Error> {
    let schemas = if !schema_qualified_name.schema_name.is_empty() {
        [&schema_qualified_name.schema_name, ""]
    } else {
        [PUBLIC_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME]
    };
    query_scalar(query)
        .bind(schemas)
        .bind(&schema_qualified_name.local_name)
        .fetch_all(pool)
        .await
}

#[cfg(test)]
mod test {}
