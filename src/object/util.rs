use crate::{join_display_iter, join_slice, map_join_slice, PgDiffError};
use serde::Deserialize;
use sqlx::postgres::types::Oid;
use std::fmt::{Display, Formatter, Write};

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
                join_slice(include.as_slice(), ",", f)?;
                write!(f, ")")?;
            }
            _ => {}
        }
        match &self.with {
            Some(storage_parameters) if !storage_parameters.is_empty() => {
                write!(f, " WITH(")?;
                map_join_slice(
                    storage_parameters.as_slice(),
                    |p, f| {
                        write!(f, "{p}")?;
                        Ok(())
                    },
                    ",",
                    f,
                )?;
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
        Self {
            old,
            new,
        }
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

pub fn compare_option_lists<O, W>(
    object_type_name: &str,
    object_name: &SchemaQualifiedName,
    old: Option<&[O]>,
    new: Option<&[O]>,
    w: &mut W,
) -> Result<(), PgDiffError>
where
    O: Display + PartialEq,
    W: Write,
{
    if let Some(new_options) = new {
        let old_options = old.unwrap_or_default();
        let set_options = new_options.iter().filter(|p| old_options.contains(*p));
        write!(w, "ALTER {} {} SET (", object_type_name, object_name)?;
        join_display_iter(set_options, ",", w)?;
        writeln!(w, ");")?;
    }
    if let Some(old_options) = old {
        let new_options = new.unwrap_or_default();
        write!(w, "ALTER {} {} RESET (", object_type_name, object_name)?;
        for p in old_options.iter().filter(|p| new_options.contains(*p)) {
            let option = p.to_string();
            if let Some((first, _)) = option.split_once('=') {
                write!(w, "{first}")?;
            } else {
                write!(w, "{option}")?;
            }
        }
        writeln!(w, ");")?;
    }
    Ok(())
}
