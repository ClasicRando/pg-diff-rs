use std::fmt::{Display, Formatter, Write};

use serde::Deserialize;
use sqlx::postgres::types::Oid;
use sqlx::{query_as, PgPool};

use crate::object::{IndexParameters, SchemaQualifiedName, SqlObject};
use crate::{write_join, PgDiffError};

/// Fetch all constraints within the current database for the specified tables (by OID)
pub async fn get_constraints(
    pool: &PgPool,
    tables: &[Oid],
) -> Result<Vec<Constraint>, PgDiffError> {
    let constraints_query = include_str!("./../../queries/constraints.pgsql");
    let constraints = match query_as(constraints_query)
        .bind(tables)
        .fetch_all(pool)
        .await
    {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load constraints");
            return Err(error.into());
        },
    };
    Ok(constraints)
}

/// Struct representing a SQL constraint object
#[derive(Debug, sqlx::FromRow)]
pub struct Constraint {
    /// OID of the owning table
    pub(crate) table_oid: Oid,
    /// Full name of the owning table
    #[sqlx(json)]
    pub(crate) owner_table_name: SchemaQualifiedName,
    /// Constraint name (local to table)
    pub(crate) name: String,
    /// Full name of the schema as a part of the table
    #[sqlx(json)]
    pub(crate) schema_qualified_name: SchemaQualifiedName,
    /// Constraint variant information
    #[sqlx(json)]
    pub(crate) constraint_type: ConstraintType,
    /// Constraint firing timing
    #[sqlx(json)]
    pub(crate) timing: ConstraintTiming,
    /// Dependencies of the constraint
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl PartialEq for Constraint {
    #[inline]
    fn eq(&self, other: &Constraint) -> bool {
        self.owner_table_name == other.owner_table_name
            && self.name == other.name
            && self.schema_qualified_name == other.schema_qualified_name
            && self.constraint_type == other.constraint_type
            && self.timing == other.timing
    }
}

impl SqlObject for Constraint {
    fn name(&self) -> &SchemaQualifiedName {
        &self.schema_qualified_name
    }

    fn object_type_name(&self) -> &str {
        "CONSTRAINT"
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        match &self.constraint_type {
            ConstraintType::Check {
                expression,
                is_inheritable,
                ..
            } => write!(
                w,
                "ALTER TABLE {} ADD CONSTRAINT {}\nCHECK({}){} ",
                self.owner_table_name,
                self.name,
                expression.trim(),
                if *is_inheritable { "" } else { " NO INHERIT" }
            )?,
            ConstraintType::Unique {
                columns,
                are_nulls_distinct,
                index_parameters,
            } => {
                write!(
                    w,
                    "ALTER TABLE {} ADD CONSTRAINT {}\nUNIQUE NULLS{} DISTINCT (",
                    self.owner_table_name,
                    self.name,
                    if *are_nulls_distinct { "" } else { " NOT" },
                )?;
                write_join!(w, columns, ",");
                write!(w, "){index_parameters} ")?;
            },
            ConstraintType::PrimaryKey {
                columns,
                index_parameters,
            } => {
                write!(
                    w,
                    "ALTER TABLE {} ADD CONSTRAINT {}\nPRIMARY KEY (",
                    self.owner_table_name, self.name,
                )?;
                write_join!(w, columns, ",");
                write!(w, "){index_parameters} ")?;
            },
            ConstraintType::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                match_type,
                on_delete,
                on_update,
            } => {
                write!(
                    w,
                    "ALTER TABLE {} ADD CONSTRAINT {}\nFOREIGN KEY (",
                    self.owner_table_name, self.name,
                )?;
                write_join!(w, columns, ",");
                write!(w, ") REFERENCES {ref_table}(")?;
                write_join!(w, ref_columns, ",");
                write!(
                    w,
                    ") {}\n    ON DELETE {on_delete}\n    ON UPDATE {on_update}\n",
                    match_type.as_ref(),
                )?;
            },
        };
        writeln!(w, "{};", self.timing)?;
        Ok(())
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.constraint_type != new.constraint_type {
            self.drop_statements(w)?;
            new.create_statements(w)?;
            return Ok(());
        }

        if self.timing != new.timing {
            writeln!(
                w,
                "ALTER TABLE {} ALTER CONSTRAINT {} {};",
                self.owner_table_name, self.name, new.timing
            )?;
        }

        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(
            w,
            "ALTER TABLE {} DROP CONSTRAINT {};",
            self.owner_table_name, self.name
        )?;
        Ok(())
    }
}

/// Constraint variants and their respective details
#[derive(Debug, Deserialize, PartialEq, Clone)]
#[serde(tag = "type")]
pub enum ConstraintType {
    /// `CHECK` table/column constraint. If the number of columns is 1, then it's a column
    /// constraint. Otherwise, it's a table constraint.
    Check {
        /// Columns associated with the constraint
        columns: Vec<String>,
        /// Expression executed to validate the constraint criteria is met
        expression: String,
        /// True if the constraint can be inherited by child tables
        is_inheritable: bool,
    },
    /// `UNIQUE` table/column constraint. If the number of columns is 1, then it's a column
    /// constraint. Otherwise, it's a table constraint.
    Unique {
        /// Columns associated with the constraint
        columns: Vec<String>,
        /// True if null values in the columns are considered distinct from each other
        are_nulls_distinct: bool,
        /// Parameters used to store the index
        index_parameters: IndexParameters,
    },
    /// `PRIMARY KEY` table constraint
    PrimaryKey {
        /// Columns associated with the constraint
        columns: Vec<String>,
        /// Parameters used to store the index
        index_parameters: IndexParameters,
    },
    /// `FOREIGN KEY` table/column constraint. If the number of columns is 1, then it's a column
    /// constraint. Otherwise, it's a table constraint.
    ForeignKey {
        /// Columns associated with the constraint in the source table
        columns: Vec<String>,
        /// Full name of the referenced table
        ref_table: SchemaQualifiedName,
        /// Columns checked against in the referenced table. Count is always the same as the source
        /// table columns
        ref_columns: Vec<String>,
        /// Match type of the foreign key
        match_type: ForeignKeyMatch,
        /// Action performed when the referenced record is deleted
        on_delete: ForeignKeyAction,
        /// Action performed when the referenced record is updated
        on_update: ForeignKeyAction,
    },
}

/// Constraint timing as deferrable or not deferrable
#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ConstraintTiming {
    #[default]
    NotDeferrable,
    Deferrable {
        /// True if the constraint is immediately deferrable
        is_immediate: bool,
    },
}

impl Display for ConstraintTiming {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            ConstraintTiming::NotDeferrable => "NOT DEFERRABLE",
            ConstraintTiming::Deferrable { is_immediate } => {
                if *is_immediate {
                    "DEFERRABLE INITIALLY IMMEDIATE"
                } else {
                    "DEFERRABLE INITIALLY DEFERRED"
                }
            },
        };
        f.write_str(text)
    }
}

/// Foreign key match options
#[derive(Debug, Default, Deserialize, PartialEq, strum::AsRefStr, Clone)]
pub enum ForeignKeyMatch {
    /// If the foreign key is multi-column, all fields must be null to ignore matching. If the
    /// foreign key is single-column, then this option does not differ from
    /// [ForeignKeyMatch::Simple]
    #[strum(serialize = "MATCH FULL")]
    Full,
    /// Available but not yet implemented in postgresql as of Postgres 16
    #[strum(serialize = "MATCH PARTIAL")]
    Partial,
    /// Any field within the foreign key columns (single or multi) can be null for matching to be
    /// ignored.
    #[default]
    #[strum(serialize = "MATCH SIMPLE")]
    Simple,
}

/// Foreign key action when referenced record changes
#[derive(Debug, Default, Deserialize, PartialEq, Clone)]
#[serde(tag = "type")]
pub enum ForeignKeyAction {
    /// Produces an error if the referenced record changes. The foreign key can then be deferred
    #[default]
    NoAction,
    /// Produces an error if the referenced record changes. The foreign key cannot be deferred
    Restrict,
    /// Cascade the operation to the source table's record (e.g. delete if referenced record is
    /// deleted)
    Cascade,
    /// Set the referencing column(s) in the source table as null
    SetNull {
        /// Optional subset of columns within the referenced columns to set null. This is only valid
        /// for `ON DELETE` actions
        columns: Option<Vec<String>>,
    },
    /// Set the referencing column(s) in the source table as their default value
    SetDefault {
        /// Optional subset of columns within the referenced columns to set to their default value.
        /// This is only valid for `ON DELETE` actions
        columns: Option<Vec<String>>,
    },
}

impl Display for ForeignKeyAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ForeignKeyAction::NoAction => write!(f, "NO ACTION"),
            ForeignKeyAction::Restrict => write!(f, "RESTRICT"),
            ForeignKeyAction::Cascade => write!(f, "CASCADE"),
            ForeignKeyAction::SetNull { columns } => {
                if let Some(columns) = columns {
                    write!(f, "SET NULL (")?;
                    write_join!(f, columns, ",");
                    write!(f, ")")
                } else {
                    write!(f, "SET NULL")
                }
            },
            ForeignKeyAction::SetDefault { columns } => {
                if let Some(columns) = columns {
                    write!(f, "SET DEFAULT (")?;
                    write_join!(f, columns, ",");
                    write!(f, ")")
                } else {
                    write!(f, "SET DEFAULT")
                }
            },
        }
    }
}

#[cfg(test)]
mod test {
    use sqlx::postgres::types::Oid;

    use crate::object::{IndexParameters, SchemaQualifiedName, SqlObject};

    use super::{Constraint, ConstraintTiming, ConstraintType, ForeignKeyAction, ForeignKeyMatch};
    static SCHEMA: &str = "test_schema";
    static TABLE: &str = "test_table";
    static REF_TABLE: &str = "ref_table";
    static NAME: &str = "test_constraint";
    static TEST_COL: &str = "test_col";
    static TEST_COL2: &str = "test_col2";

    fn create_constraint(
        schema_name: &str,
        table_name: &str,
        constraint_name: &str,
        constraint_type: ConstraintType,
        timing: ConstraintTiming,
    ) -> Constraint {
        Constraint {
            table_oid: Oid(1),
            owner_table_name: SchemaQualifiedName::new(schema_name, table_name),
            name: constraint_name.into(),
            schema_qualified_name: SchemaQualifiedName::from(format!(
                "{schema_name}.{table_name}.{constraint_name}"
            )),
            constraint_type,
            timing,
            dependencies: vec![],
        }
    }

    #[rstest::rstest]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Check {
                columns: vec![TEST_COL.into()],
                expression: "test_col = 'test'".into(),
                is_inheritable: false
            },
            ConstraintTiming::NotDeferrable
        ),
        include_str!("../../test-files/sql/constraint-create-case1.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Check {
                columns: vec![TEST_COL.into(), TEST_COL2.into()],
                expression: String::from("test_col = 'test' AND test_col = test_col2"),
                is_inheritable: true
            },
            ConstraintTiming::NotDeferrable
        ),
        include_str!("../../test-files/sql/constraint-create-case2.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Unique {
                columns: vec![TEST_COL.into()],
                are_nulls_distinct: true,
                index_parameters: IndexParameters {
                    include: None,
                    with: None,
                    tablespace: None
                },
            },
            ConstraintTiming::Deferrable { is_immediate: true }
        ),
        include_str!("../../test-files/sql/constraint-create-case3.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Unique {
                columns: vec![TEST_COL.into(), TEST_COL2.into()],
                are_nulls_distinct: false,
                index_parameters: IndexParameters {
                    include: None,
                    with: None,
                    tablespace: None
                },
            },
            ConstraintTiming::Deferrable { is_immediate: false }
        ),
        include_str!("../../test-files/sql/constraint-create-case4.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::PrimaryKey {
                columns: vec![TEST_COL.into()],
                index_parameters: IndexParameters {
                    include: None,
                    with: None,
                    tablespace: None,
                },
            },
            ConstraintTiming::NotDeferrable,
        ),
        include_str!("../../test-files/sql/constraint-create-case5.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::PrimaryKey {
                columns: vec![TEST_COL.into(), TEST_COL2.into()],
                index_parameters: IndexParameters {
                    include: None,
                    with: None,
                    tablespace: None,
                },
            },
            ConstraintTiming::NotDeferrable,
        ),
        include_str!("../../test-files/sql/constraint-create-case6.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::ForeignKey {
                columns: vec![TEST_COL.into()],
                ref_table: SchemaQualifiedName::new(SCHEMA, REF_TABLE),
                ref_columns: vec![TEST_COL.into()],
                match_type: ForeignKeyMatch::Full,
                on_delete: ForeignKeyAction::Cascade,
                on_update: ForeignKeyAction::NoAction,
            },
            ConstraintTiming::NotDeferrable,
        ),
        include_str!("../../test-files/sql/constraint-create-case7.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::ForeignKey {
                columns: vec![TEST_COL.into(), TEST_COL2.into()],
                ref_table: SchemaQualifiedName::new(SCHEMA, REF_TABLE),
                ref_columns: vec![TEST_COL.into(), TEST_COL2.into()],
                match_type: ForeignKeyMatch::Simple,
                on_delete: ForeignKeyAction::Restrict,
                on_update: ForeignKeyAction::SetDefault { columns: None },
            },
            ConstraintTiming::NotDeferrable,
        ),
        include_str!("../../test-files/sql/constraint-create-case8.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::ForeignKey {
                columns: vec![TEST_COL.into(), TEST_COL2.into()],
                ref_table: SchemaQualifiedName::new(SCHEMA, REF_TABLE),
                ref_columns: vec![TEST_COL.into(), TEST_COL2.into()],
                match_type: ForeignKeyMatch::Partial,
                on_delete: ForeignKeyAction::SetNull { columns: Some(vec![TEST_COL.into()]) },
                on_update: ForeignKeyAction::SetDefault { columns: Some(vec![TEST_COL.into()]) },
            },
            ConstraintTiming::NotDeferrable,
        ),
        include_str!("../../test-files/sql/constraint-create-case9.pgsql"),
    )]
    fn create_statements_should_add_alter_table_add_constraint_statement(
        #[case] constraint: Constraint,
        #[case] statement: &str,
    ) {
        let mut writable = String::new();
        constraint.create_statements(&mut writable).unwrap();

        assert_eq!(statement.trim(), writable.trim());
    }

    #[test]
    fn alter_statements_should_add_alter_table_alter_constraint_when_changed_timing() {
        let constraint_type = ConstraintType::Unique {
            columns: vec![],
            are_nulls_distinct: true,
            index_parameters: IndexParameters {
                include: None,
                with: None,
                tablespace: None,
            },
        };
        let constraint_before = create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            constraint_type.clone(),
            ConstraintTiming::NotDeferrable,
        );
        let constraint_after = create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            constraint_type,
            ConstraintTiming::Deferrable { is_immediate: true },
        );
        let statement = include_str!("../../test-files/sql/constraint-alter-changed-timing.pgsql");
        let mut writable = String::new();

        constraint_before
            .alter_statements(&constraint_after, &mut writable)
            .unwrap();

        assert_eq!(statement.trim(), writable.trim());
    }

    #[rstest::rstest]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Check {
                columns: vec![TEST_COL.into()],
                expression: "test_col = 'test'".into(),
                is_inheritable: false
            },
            ConstraintTiming::NotDeferrable
        ),
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Check {
                columns: vec![TEST_COL2.into()],
                expression: "test_col2 = 'test'".into(),
                is_inheritable: false
            },
            ConstraintTiming::NotDeferrable
        ),
        include_str!("../../test-files/sql/constraint-alter-changed-type-case1.pgsql"),
    )]
    #[case(
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Check {
                columns: vec![TEST_COL.into()],
                expression: "test_col = 'test'".into(),
                is_inheritable: false
            },
            ConstraintTiming::NotDeferrable
        ),
        create_constraint(
            SCHEMA,
            TABLE,
            NAME,
            ConstraintType::Unique {
                columns: vec![TEST_COL.into()],
                are_nulls_distinct: false,
                index_parameters: IndexParameters {
                    include: None,
                    with: None,
                    tablespace: None
                }
            },
            ConstraintTiming::NotDeferrable
        ),
        include_str!("../../test-files/sql/constraint-alter-changed-type-case2.pgsql"),
    )]
    fn alter_statements_should_add_drop_and_create_constraint_statements(
        #[case] old_constraint: Constraint,
        #[case] new_constraint: Constraint,
        #[case] statement: &str,
    ) {
        let mut writable = String::new();

        old_constraint
            .alter_statements(&new_constraint, &mut writable)
            .unwrap();

        assert_eq!(statement.trim(), writable.trim());
    }
}
