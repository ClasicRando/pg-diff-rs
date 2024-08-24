use std::fmt::Write;

use serde::Deserialize;

use crate::PgDiffError;

use super::{SchemaQualifiedName, SqlObject};

#[derive(Debug, Deserialize, PartialEq)]
pub struct Policy {
    name: String,
    schema_qualified_name: SchemaQualifiedName,
    owner_table: SchemaQualifiedName,
    is_permissive: bool,
    applies_to: Vec<String>,
    command: PolicyCommand,
    check_expression: Option<String>,
    using_expression: Option<String>,
    columns: Vec<String>,
}

impl SqlObject for Policy {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "POLICY"
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        write!(
            w,
            "CREATE POLICY {} ON {} AS {} FOR {} TO {}",
            self.name,
            self.owner_table,
            if self.is_permissive {
                "PERMISSIVE"
            } else {
                "RESTRICTIVE"
            },
            self.command.as_ref(),
            self.applies_to.join(" ")
        )?;
        if let Some(using_expression) = &self.using_expression {
            write!(w, " USING ({using_expression})")?;
        }
        if let Some(check_expression) = &self.check_expression {
            write!(w, " WITH CHECK ({check_expression})")?;
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.is_permissive != new.is_permissive || self.command != new.command {
            self.drop_statements(w)?;
            self.create_statements(w)?;
            return Ok(());
        }
        write!(
            w,
            "ALTER POLICY {} ON {} TO {}",
            self.name,
            self.owner_table,
            new.applies_to.join(" ")
        )?;
        if let Some(using_expression) = &new.using_expression {
            write!(w, " USING ({using_expression})")?;
        }
        if let Some(check_expression) = &new.check_expression {
            write!(w, " WITH CHECK ({check_expression})")?;
        }
        writeln!(w, ";")?;
        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP POLICY {} ON {};", self.name, self.owner_table)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, PartialEq, strum::AsRefStr)]
pub enum PolicyCommand {
    #[serde(rename = "r")]
    #[strum(serialize = "SELECT")]
    Select,
    #[serde(rename = "a")]
    #[strum(serialize = "INSERT")]
    Insert,
    #[serde(rename = "w")]
    #[strum(serialize = "UPDATE")]
    Update,
    #[serde(rename = "d")]
    #[strum(serialize = "DELETE")]
    Delete,
    #[serde(rename = "*")]
    #[strum(serialize = "ALL")]
    All,
}
