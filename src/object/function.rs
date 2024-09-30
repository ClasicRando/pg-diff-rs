use std::borrow::Cow;
use std::fmt::{Display, Formatter, Write};

use lazy_regex::regex;
use serde::Deserialize;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgTypeInfo, PgValueRef};
use sqlx::{query_as, Decode, PgPool, Postgres};

use crate::object::plpgsql::{parse_plpgsql_function, PlPgSqlFunction};
use crate::object::table::get_table_by_qualified_name;
use crate::{write_join, PgDiffError};

use super::{
    check_names_in_database, compare_option_lists, is_verbose, OptionListObject,
    SchemaQualifiedName, SqlObject, PG_CATALOG_SCHEMA_NAME,
};

/// Fetch all functions within the `schemas` specified
pub async fn get_functions(pool: &PgPool, schemas: &[&str]) -> Result<Vec<Function>, PgDiffError> {
    let functions_query = include_str!("../../queries/functions.pgsql");
    let functions = match query_as(functions_query)
        .bind(schemas)
        .fetch_all(pool)
        .await
    {
        Ok(inner) => inner,
        Err(error) => {
            println!("Could not load functions");
            return Err(error.into());
        },
    };
    Ok(functions)
}

/// Fetch all functions that match the provided `schema_qualified_name`. If the schema portion of
/// the name is not supplied (e.g. the referenced name is a builtin function) then supply the
/// schemas to search as `public` and `pg_catalog`.
async fn get_functions_by_qualified_name(
    pool: &PgPool,
    schema_qualified_name: &SchemaQualifiedName,
) -> Result<Vec<SchemaQualifiedName>, PgDiffError> {
    let functions_query = include_str!("../../queries/dependency_functions.pgsql");
    let functions =
        match check_names_in_database(pool, schema_qualified_name, functions_query).await {
            Ok(inner) => inner,
            Err(error) => {
                if is_verbose() {
                    println!("Could not load functions by qualified name");
                }
                return Err(error.into());
            },
        };
    Ok(functions)
}

/// Fetch all objects that match the provided `schema_qualified_name`. If the schema portion of the
/// name is not supplied (e.g. the referenced name is a builtin object) then supply the schemas to
/// search as `public` and `pg_catalog`.
async fn get_objects_by_qualified_name(
    pool: &PgPool,
    schema_qualified_name: &SchemaQualifiedName,
) -> Result<Vec<SchemaQualifiedName>, PgDiffError> {
    let all_objects_query = include_str!("../../queries/all_objects.pgsql");
    let objects =
        match check_names_in_database(pool, schema_qualified_name, all_objects_query).await {
            Ok(inner) => inner,
            Err(error) => {
                if is_verbose() {
                    println!("Could not load objects by qualified name");
                }
                return Err(error.into());
            },
        };
    Ok(objects)
}

/// Postgresql function arguments
pub struct FunctionArgument<'s> {
    /// Argument index (zero-based)
    index: usize,
    /// Name of the argument. Blank if unnamed
    arg_name: &'s str,
    /// Data type of the argument
    arg_type: &'s str,
    /// Default expression of the argument. Blank if no default specified
    default_expression: &'s str,
}

impl<'s> FunctionArgument<'s> {
    /// Create list of [FunctionArgument] from the string specified as the comma separated list of
    /// arguments as seen in a `CREATE FUNCTION` statement.
    fn from_arg_list(args: &'s str) -> Vec<Self> {
        if args.is_empty() {
            return vec![];
        }
        args
            .split(',')
            .enumerate()
            .map(|(i, arg)| {
                let parts: Vec<&str> = arg.split_whitespace().collect();
                let (_, arg_name, arg_type, default_expression) = match parts.as_slice() {
                    [arg_mode, arg_name, arg_type, "DEFAULT", default_expression] => (*arg_mode, *arg_name, *arg_type, *default_expression),
                    [arg_mode, arg_type, "DEFAULT", default_expression] => (*arg_mode, "", *arg_type, *default_expression),
                    [arg_type, "DEFAULT", default_expression] => ("IN", "", *arg_type, *default_expression),
                    [arg_mode, arg_name, arg_type] => (*arg_mode, *arg_name, *arg_type, ""),
                    [arg_mode, arg_type] => (*arg_mode, "", *arg_type, ""),
                    [arg_type] => ("IN", "", *arg_type, ""),
                    _ => {
                        panic!("Core assertion of postgresql syntax has been violated for args = {args:?}, arg = {arg:?}");
                    }
                };
                FunctionArgument {
                    index: i + 1,
                    arg_name,
                    arg_type,
                    default_expression
                }
            })
            .collect()
    }

    /// Name of the argument. If the argument is unnamed then `param{index}` is returned.
    fn argument_name(&self) -> String {
        if self.arg_name.is_empty() {
            return format!("param{}", self.index);
        }
        self.arg_name.to_owned()
    }
}

impl<'s> Display for FunctionArgument<'s> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.arg_name.is_empty() {
            write!(f, "param{}", self.index)?;
        } else {
            write!(f, "{}", self.arg_name)?;
        }
        write!(f, " {}", self.arg_type)?;
        if !self.default_expression.is_empty() {
            write!(f, " = {}", self.default_expression)?;
        }
        Ok(())
    }
}

/// Postgresql function object. This includes procedures which are highlighted with the
/// `is_procedure` field.
#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Function {
    /// Full name of the function
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    /// True if this is a stored procedure (i.e. no return value)
    pub(crate) is_procedure: bool,
    /// Number of arguments
    pub(crate) input_arg_count: i16,
    /// Names of the arguments. All unnamed parameters will be empty string. If all arguments are
    /// unnamed then this will be [None].
    pub(crate) arg_names: Option<Vec<String>>,
    /// Declaration block for the function arguments as returned from
    /// `pg_catalog.pg_get_function_arguments`
    pub(crate) arguments: String,
    /// Return type of the function as returned from `pg_catalog.pg_get_function_result`
    pub(crate) return_type: Option<String>,
    /// Estimated cost of function execution (in most cases this is only generated by the server)
    pub(crate) estimated_cost: f32,
    /// Estimated cost of function execution (in most cases this is only generated by the server)
    pub(crate) estimated_rows: Option<f32>,
    /// Function security option
    pub(crate) security: FunctionSecurity,
    /// True if the function has no side effects and does not expose the arguments other than
    /// possibly by the return value.
    pub(crate) is_leak_proof: bool,
    /// Function strictness option
    pub(crate) strict: FunctionStrict,
    /// Function execution behaviour option
    pub(crate) behaviour: FunctionBehaviour,
    /// Function parallelism option
    pub(crate) parallel: FunctionParallel,
    /// Function source code details
    #[sqlx(json)]
    pub(crate) source_code: FunctionSourceCode,
    /// Function configuration option
    pub(crate) config: Option<Vec<String>>,
    /// Function dependencies found in database. This can be updated later is `source_code` can be
    /// analyzed.
    #[sqlx(json)]
    pub(crate) dependencies: Vec<SchemaQualifiedName>,
}

impl Function {
    /// Attempt to extract additional dependencies if the source code of the procedure is executed
    /// at runtime.
    ///
    /// This is only valid for non-parsed SQL and pl/pgsql functions since the code is only
    /// evaluated at function creation and execution time (i.e. dependencies are not tracked which
    /// is the case for parsed SQL functions).
    ///
    /// ## Errors
    /// - if the SQL source code cannot be analyzed (this should not happen unless the source code
    ///     is invalid)
    /// - searching the database for SQL objects referenced fails
    pub async fn extract_more_dependencies(&mut self, pool: &PgPool) -> Result<(), PgDiffError> {
        if let FunctionSourceCode::Sql {
            source,
            is_pre_parsed,
        } = &self.source_code
        {
            if *is_pre_parsed {
                return Ok(());
            }
            let result = pg_query::parse(source.trim()).map_err(|e| PgDiffError::PgQuery {
                object_name: self.name.clone(),
                error: e,
            })?;
            for table in result.tables() {
                let table_name = SchemaQualifiedName::from(&table);
                let tables = get_table_by_qualified_name(pool, &table_name).await?;
                self.add_dependencies_if_match(&table_name, tables);
            }
            for function in result.functions() {
                let function_name = SchemaQualifiedName::from(&function);
                let functions = get_functions_by_qualified_name(pool, &function_name).await?;
                self.add_dependencies_if_match(&function_name, functions);
            }
        }
        if let FunctionSourceCode::Plpgsql { .. } = &self.source_code {
            let mut block = String::new();
            self.create_statement(&mut block, true)?;
            let result: Vec<PlPgSqlFunction> = match parse_plpgsql_function(&block) {
                Ok(inner) => inner,
                Err(error) => {
                    if is_verbose() {
                        println!("Object Name: {}. {error}\n", self.name);
                    }
                    return Ok(());
                },
            };
            for function in result {
                let names = match function.get_objects() {
                    Ok(inner) => inner,
                    Err(error) => {
                        if is_verbose() {
                            println!("Could not get dependencies of dynamic function {} due to object extraction error. {error}", self.name);
                        }
                        return Ok(());
                    },
                };
                for name in names {
                    let objects = get_objects_by_qualified_name(pool, &name).await?;
                    self.add_dependencies_if_match(&name, objects);
                }
            }
        }
        Ok(())
    }

    /// Add additional dependencies to the function object.
    ///
    /// Only cases where a single object is found for a given qualified name are actually added. If
    /// multiple objects are found then they are ignored since we do not currently support checking
    /// function overloads.
    fn add_dependencies_if_match(
        &mut self,
        name: &SchemaQualifiedName,
        objects: Vec<SchemaQualifiedName>,
    ) {
        match &objects[..] {
            [object] => {
                if object.schema_name == PG_CATALOG_SCHEMA_NAME {
                    return;
                }
                if is_verbose() {
                    println!(
                        "Adding {} as dependency for dynamic function {}",
                        object, self.name
                    );
                }
                self.dependencies.push(object.clone());
            },
            [] => {
                if is_verbose() {
                    println!(
                        "Could not match object {name} to an object for {}. Skipping for now.",
                        self.name
                    )
                }
            },
            objects => {
                if objects
                    .iter()
                    .all(|d| d.schema_name == PG_CATALOG_SCHEMA_NAME)
                {
                    return;
                }
                if is_verbose() {
                    println!(
                        "Found multiple matches for {name} to an object for {}. {:?}",
                        self.name,
                        objects.to_vec()
                    );
                }
            },
        }
    }

    /// Rewrite the `arguments` list to use placeholder names for unnamed arguments.
    fn rewrite_arguments<W>(&self, w: &mut W) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        let arguments = FunctionArgument::from_arg_list(&self.arguments);
        write_join!(w, arguments, ",");
        Ok(())
    }

    /// Write the `CREATE` statement to the writable object.
    ///
    /// Optionally modify code if `rewrite_code` is true. This option should only be used when
    /// trying to analyze functions because otherwise, the function created won't match the intended
    /// source code.
    fn create_statement<W>(&self, w: &mut W, rewrite_code: bool) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        write!(
            w,
            "CREATE OR REPLACE {} {} (",
            self.object_type_name(),
            self.name,
        )?;

        if rewrite_code {
            self.rewrite_arguments(w)?;
        } else {
            w.write_str(&self.arguments)?;
        }
        w.write_str(")\n")?;

        if let Some(returns) = &self.return_type {
            writeln!(w, "RETURNS {returns}")?;
        }
        writeln!(w, "LANGUAGE {}", self.source_code.language())?;
        if !self.is_procedure {
            writeln!(
                w,
                "{}\n{}LEAKPROOF\n{}\n{}\nCOST {}",
                self.behaviour.as_ref(),
                if self.is_leak_proof { "" } else { "NOT " },
                self.strict.as_ref(),
                self.parallel.as_ref(),
                self.estimated_cost
            )?;
            if let Some(estimated_rows) = &self.estimated_rows {
                writeln!(w, "ROWS {estimated_rows}")?;
            }
        }
        writeln!(w, "{}", self.security.as_ref())?;
        if let Some(config) = &self.config {
            for parameter in config {
                writeln!(w, "SET {parameter}")?;
            }
        }

        let arguments = if rewrite_code {
            Some(FunctionArgument::from_arg_list(&self.arguments))
        } else {
            None
        };
        self.source_code.format(w, arguments)?;

        Ok(())
    }
}

impl OptionListObject for Function {
    /// Override the alter prefix to include the variance in object type name (`FUNCTION` vs
    /// `PROCEDURE`) and the required argument list to distinguish between function overloads when
    /// altering.
    fn write_alter_prefix<W>(&self, w: &mut W) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        write!(
            w,
            "ALTER {} {}({})",
            self.object_type_name(),
            self.name,
            self.arguments
        )?;
        Ok(())
    }
}

impl SqlObject for Function {
    fn name(&self) -> &SchemaQualifiedName {
        &self.name
    }

    fn object_type_name(&self) -> &str {
        if self.is_procedure {
            "PROCEDURE"
        } else {
            "FUNCTION"
        }
    }

    fn dependencies(&self) -> &[SchemaQualifiedName] {
        &self.dependencies
    }

    fn create_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        self.create_statement(w, false)
    }

    fn alter_statements<W: Write>(&self, new: &Self, w: &mut W) -> Result<(), PgDiffError> {
        if self.arguments != new.arguments || self.return_type != new.return_type {
            self.drop_statements(w)?;
            self.create_statements(w)?;
            return Ok(());
        }

        if self.security != new.security {
            writeln!(
                w,
                "ALTER FUNCTION {}({}) {};",
                self.name,
                self.arguments,
                new.security.as_ref()
            )?;
        }

        compare_option_lists(self, self.config.as_deref(), new.config.as_deref(), w)?;

        if self.is_procedure {
            return Ok(());
        }

        if self.strict != new.strict {
            writeln!(
                w,
                "ALTER FUNCTION {}({}) {};",
                self.name,
                self.arguments,
                new.strict.as_ref()
            )?;
        }
        if self.behaviour != new.behaviour {
            writeln!(
                w,
                "ALTER FUNCTION {}({}) {};",
                self.name,
                self.arguments,
                new.behaviour.as_ref()
            )?;
        }
        if self.is_leak_proof != new.is_leak_proof {
            writeln!(
                w,
                "ALTER FUNCTION {}({}) {}LEAKPROOF;",
                self.name,
                self.arguments,
                if new.is_leak_proof { "" } else { "NOT " }
            )?;
        }
        if self.parallel != new.parallel {
            writeln!(
                w,
                "ALTER FUNCTION {}({}) {};",
                self.name,
                self.arguments,
                new.parallel.as_ref()
            )?;
        }
        if self.estimated_cost != new.estimated_cost {
            writeln!(
                w,
                "ALTER FUNCTION {}({}) COST {};",
                self.name, self.arguments, new.estimated_cost
            )?;
        }
        match (&self.estimated_rows, &new.estimated_rows) {
            (Some(old_estimated_rows), Some(new_estimated_rows))
                if old_estimated_rows != new_estimated_rows =>
            {
                writeln!(
                    w,
                    "ALTER FUNCTION {}({}) ROWS {new_estimated_rows};",
                    self.name, self.arguments
                )?;
            },
            (None, None) => {},
            _ => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: self.name.to_string(),
                    reason: "Cannot change the return type to/from rows".to_string(),
                })
            },
        }

        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP {} {};", self.object_type_name(), self.name)?;
        Ok(())
    }
}

/// Function source code variants.
///
/// Variants are defined by language and include the options valid for that language.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum FunctionSourceCode {
    /// Dynamically or statically executed SQL code
    Sql {
        /// SQL source code
        source: String,
        /// True if the SQL code has been pre parsed into an AST for future execution. If this is
        /// true then `source` begins with `BEGIN ATOMIC` or is a single expression beginning with a
        /// `RETURN` statement.
        is_pre_parsed: bool,
    },
    /// Dynamically executed pl/pgsql code
    Plpgsql {
        /// pl/pgsql source code
        source: String,
    },
    /// Dynamically loaded C code
    C {
        /// C function name to be invoked
        name: String,
        /// Name of the shared library file containing the compiled C function
        link_symbol: String,
    },
    /// Special variant for functions internal to Postgres
    Internal {
        /// Name of the internal function
        name: String,
    },
    /// Catchall variant for all other languages
    Invalid {
        /// Name of the function
        function_name: String,
        /// Language name of the function
        language_name: String,
    },
}

impl FunctionSourceCode {
    /// Language name of the source code
    fn language(&self) -> &str {
        match self {
            FunctionSourceCode::Sql { .. } => "sql",
            FunctionSourceCode::Plpgsql { .. } => "plpgsql",
            FunctionSourceCode::C { .. } => "c",
            FunctionSourceCode::Internal { .. } => "internal",
            FunctionSourceCode::Invalid { language_name, .. } => language_name,
        }
    }

    /// Format the source code for inclusion in a `CREATE` statement. Arguments can be supplied if
    /// the caller wishes to rewrite `pl/pgsql` source code to remove unnamed arguments.
    fn format<W>(
        &self,
        w: &mut W,
        arguments: Option<Vec<FunctionArgument>>,
    ) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        match self {
            Self::Sql {
                source,
                is_pre_parsed,
            } if *is_pre_parsed => writeln!(w, "{};", source)?,
            Self::Plpgsql { source } | Self::Sql { source, .. } => {
                w.write_str("AS $function$")?;
                if let Some(args) = arguments {
                    rewrite_plpgsql_source(w, source, args)?;
                } else {
                    writeln!(w, "{}", source.trim())?;
                }
                w.write_str("$function$;")?;
            },
            Self::C {
                name,
                link_symbol: bin_info,
            } => writeln!(w, "AS '{bin_info}', '{}';", name)?,
            Self::Internal { name } => {
                return Err(PgDiffError::UnsupportedFunctionLanguage {
                    object_name: SchemaQualifiedName::from(name),
                    language: "internal".to_string(),
                })
            },
            Self::Invalid {
                function_name,
                language_name,
            } => {
                return Err(PgDiffError::UnsupportedFunctionLanguage {
                    object_name: SchemaQualifiedName::from(function_name),
                    language: language_name.clone(),
                })
            },
        }
        Ok(())
    }
}

/// Rewrite the `pl/pgsql` definition to replace the usage of unnamed parameters with declared
/// variables.
fn rewrite_plpgsql_source<W>(
    w: &mut W,
    source: &str,
    function_arguments: Vec<FunctionArgument>,
) -> Result<(), PgDiffError>
where
    W: Write,
{
    let declare_regex = regex!("^declare"i);
    w.write_str("DECLARE\n    ")?;
    write_join!(w, function_arguments, ";\n    ");
    if !function_arguments.is_empty() {
        w.write_char(';')?;
    }
    let existing_block =
        function_arguments
            .iter()
            .fold(declare_regex.replace(source.trim(), ""), |acc, arg| {
                if !arg.arg_name.is_empty() {
                    return acc;
                }
                let arg_number = format!("${}", arg.index);
                if !acc.contains(&arg_number) {
                    return acc;
                }
                Cow::Owned(acc.replace(arg_number.as_str(), arg.argument_name().as_str()))
            });
    write!(w, "\n{}", existing_block)?;
    Ok(())
}

/// Function behaviour variant
#[derive(Debug, PartialEq, sqlx::Type, strum::AsRefStr)]
#[sqlx(type_name = "text")]
pub enum FunctionBehaviour {
    /// Function does not modify the database (i.e. no lookup or modification statements are
    /// executed)
    #[strum(serialize = "IMMUTABLE")]
    Immutable,
    /// Function can look up values within the database but does not modify that database
    #[strum(serialize = "STABLE")]
    Stable,
    /// Function makes no guarantees about output stability so optimizations cannot be performed
    #[strum(serialize = "VOLATILE")]
    Volatile,
}

/// Function parallelism variants
#[derive(Debug, Default, PartialEq, sqlx::Type, strum::AsRefStr)]
#[sqlx(type_name = "text")]
pub enum FunctionParallel {
    /// Function cannot be run in parallel mode (default)
    #[default]
    #[strum(serialize = "PARALLEL UNSAFE")]
    Unsafe,
    /// Function can be executed in parallel mode but the execution is restricted to parallel group
    /// leader
    #[strum(serialize = "PARALLEL RESTRICTED")]
    Restricted,
    /// Function is safe to run in parallel mode with no restrictions
    #[strum(serialize = "PARALLEL SAFE")]
    Safe,
}

/// Macro to implement [sqlx::Type] and [sqlx::Decode] for the specified type. This assumes the DB
/// value is a `bool` and the `$trueValue` is used as the outcome when the DB value is true.
/// Otherwise, the [Default::default] value is returned.
macro_rules! impl_type_for_bool {
    ($e:ident, $trueValue:expr) => {
        impl sqlx::Type<Postgres> for $e {
            fn type_info() -> PgTypeInfo {
                PgTypeInfo::with_name("bool")
            }
        }

        impl<'r> sqlx::Decode<'r, Postgres> for $e {
            fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
                if <bool as Decode<'r, Postgres>>::decode(value)? {
                    return Ok($trueValue);
                }
                return Ok(Default::default());
            }
        }
    };
}

/// Variants of a function's strictness
#[derive(Debug, Default, PartialEq, strum::AsRefStr)]
pub enum FunctionStrict {
    /// Default behaviour that allows a function to be called even when any input argument is null
    #[default]
    #[strum(serialize = "CALLED ON NULL INPUT")]
    Default,
    /// Override behaviour that allows a function call to be skipped when any input argument is null
    #[strum(serialize = "RETURNS NULL ON NULL INPUT")]
    Strict,
}

impl_type_for_bool!(FunctionStrict, FunctionStrict::Strict);

/// Variants of security checks when executing the function
#[derive(Debug, Default, PartialEq, strum::AsRefStr)]
pub enum FunctionSecurity {
    /// Default behaviour that checks security rules against the function caller
    #[default]
    #[strum(serialize = "SECURITY INVOKER")]
    Invoker,
    /// Override behaviour that checks security rules against the function creator
    #[strum(serialize = "SECURITY DEFINER")]
    Definer,
}

impl_type_for_bool!(FunctionSecurity, FunctionSecurity::Definer);
