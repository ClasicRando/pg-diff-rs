use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write};
use std::str::FromStr;

use lazy_regex::regex;
use sqlx::error::BoxDynError;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{PgTypeInfo, PgValueRef};
use sqlx::{query_as, Decode, PgPool, Postgres};

use crate::object::plpgsql::PlPgSqlFunction;
use crate::{write_join, PgDiffError};

use super::{
    compare_option_lists, Dependency, OptionListObject, PgCatalog, SchemaQualifiedName, SqlObject,
};

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
        }
    };
    Ok(functions)
}

pub struct FunctionArgument<'s> {
    index: usize,
    arg_name: &'s str,
    arg_type: &'s str,
    default_expression: &'s str,
}

impl<'s> FunctionArgument<'s> {
    fn from_arg_list(args: &'s str) -> Vec<Self> {
        if args.is_empty() {
            return Vec::new();
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
        f.write_char(';')
    }
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
pub struct Function {
    pub(crate) oid: Oid,
    #[sqlx(json)]
    pub(crate) name: SchemaQualifiedName,
    pub(crate) is_procedure: bool,
    pub(crate) input_arg_count: i16,
    pub(crate) arg_names: Option<Vec<String>>,
    pub(crate) arguments: String,
    pub(crate) return_type: Option<String>,
    pub(crate) language: String,
    pub(crate) estimated_cost: f32,
    pub(crate) estimated_rows: Option<f32>,
    pub(crate) security: FunctionSecurity,
    pub(crate) is_leak_proof: bool,
    pub(crate) strict: FunctionStrict,
    pub(crate) behaviour: FunctionBehaviour,
    pub(crate) parallel: FunctionParallel,
    pub(crate) source: String,
    pub(crate) bin_info: Option<String>,
    pub(crate) config: Option<Vec<String>>,
    pub(crate) is_pre_parsed: bool,
    #[sqlx(json)]
    pub(crate) dependencies: Vec<Dependency>,
}

impl Function {
    pub async fn extract_more_dependencies(
        &mut self,
        tables: &HashMap<SchemaQualifiedName, Oid>,
        functions: &HashMap<SchemaQualifiedName, Oid>,
    ) -> Result<(), PgDiffError> {
        if self.is_pre_parsed || self.language == "c" {
            return Ok(());
        }

        match self.language.as_str() {
            "sql" => {
                let result =
                    pg_query::parse(self.source.trim()).map_err(|e| PgDiffError::PgQuery {
                        object_name: self.name.clone(),
                        error: e,
                    })?;
                for table in result.tables() {
                    let table_name = SchemaQualifiedName::from_str(&table)?;
                    match tables.get(&table_name) {
                        Some(table_oid) => {
                            println!(
                                "Adding table {table_name} as dependency to function {}",
                                self.name
                            );
                            self.dependencies.push(Dependency {
                                oid: *table_oid,
                                catalog: PgCatalog::Class,
                            })
                        }
                        None => {
                            println!("Could not find table {table_name} referenced in {} within the scraped database tables. Ignoring for the time being.", self.name)
                        }
                    }
                }
                for function in result.functions() {
                    let function_name = SchemaQualifiedName::from_str(&function)?;
                    match functions.get(&function_name) {
                        Some(function_oid) => {
                            println!(
                                "Adding function {function_name} as dependency to function {}",
                                self.name
                            );
                            self.dependencies.push(Dependency {
                                oid: *function_oid,
                                catalog: PgCatalog::Proc,
                            })
                        }
                        None => {
                            println!("Could not find function {function_name} referenced in {} within the scraped database function. Ignoring for the time being.", self.name)
                        }
                    }
                }
            }
            "plpgsql" => {
                let mut block = String::new();
                self.create_statement(&mut block, true)?;
                let result = match pg_query::parse_plpgsql(&block) {
                    Ok(inner) => inner,
                    Err(error) => {
                        println!("Couldn't get dependencies of dynamic function {} due to parsing error. {error}", self.name);
                        return Ok(());
                    }
                };
                let result: Vec<PlPgSqlFunction> = match serde_json::from_value(result.clone()) {
                    Ok(inner) => inner,
                    Err(error) => {
                        println!("{result}");
                        println!("plpg/sql ast cannot be parsed for {}. {error}\n", self.name);
                        return Ok(());
                    }
                };
                println!("{:?}\n", result);
            }
            _ => {
                return Err(PgDiffError::General(format!(
                    "Unsupported function language to parse: {}",
                    self.language
                )))
            }
        }
        Ok(())
    }

    fn rewrite_arguments<W>(&self, w: &mut W) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        let arguments = FunctionArgument::from_arg_list(&self.arguments);
        write_join!(w, arguments.iter(), ",");
        Ok(())
    }

    fn rewrite_source<W>(&self, w: &mut W) -> Result<(), PgDiffError>
    where
        W: Write,
    {
        let declare_regex = regex!("^declare"i);
        let arguments = FunctionArgument::from_arg_list(&self.arguments);
        w.write_str("DECLARE\n\t")?;
        write_join!(w, arguments.iter(), "\n\t");
        let existing_block =
            arguments
                .iter()
                .fold(declare_regex.replace(self.source.trim(), ""), |acc, arg| {
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
            w.write_str(&self.arguments)?;
        } else {
            self.rewrite_arguments(w)?;
        }
        w.write_str(")\n")?;

        if let Some(returns) = &self.return_type {
            writeln!(w, "RETURNS {returns}")?;
        }
        writeln!(w, "LANGUAGE {}", self.language)?;
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
        match self.language.as_str() {
            "sql" if self.is_pre_parsed => writeln!(w, "{};", self.source)?,
            "plpgsql" | "sql" => {
                writeln!(w, "AS ${}$", self.object_type_name())?;
                if rewrite_code {
                    self.rewrite_source(w)?;
                } else {
                    writeln!(w, "{}", self.source.trim())?;
                }
                writeln!(w, "${}$;", self.object_type_name())?;
            }
            "c" => {
                if let Some(bin_info) = &self.bin_info {
                    writeln!(w, "AS '{bin_info}', '{}';", self.source)?
                } else {
                    return Err(PgDiffError::General(
                        "C Function is missing required pg_proc.probin value".to_string(),
                    ));
                }
            }
            _ => {
                return Err(PgDiffError::General(format!(
                    "Unsupported function language to process: {}",
                    self.language
                )))
            }
        }

        Ok(())
    }
}

impl OptionListObject for Function {
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

    fn dependency_declaration(&self) -> Dependency {
        Dependency {
            oid: self.oid,
            catalog: PgCatalog::Proc,
        }
    }

    fn dependencies(&self) -> &[Dependency] {
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
            }
            (None, None) => {}
            _ => {
                return Err(PgDiffError::InvalidMigration {
                    object_name: self.name.to_string(),
                    reason: "Cannot change the return type to/from rows".to_string(),
                })
            }
        }

        Ok(())
    }

    fn drop_statements<W: Write>(&self, w: &mut W) -> Result<(), PgDiffError> {
        writeln!(w, "DROP {} {};", self.object_type_name(), self.name)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, sqlx::Type, strum::AsRefStr)]
#[sqlx(type_name = "text")]
pub enum FunctionBehaviour {
    #[strum(serialize = "IMMUTABLE")]
    Immutable,
    #[strum(serialize = "STABLE")]
    Stable,
    #[strum(serialize = "VOLATILE")]
    Volatile,
}

#[derive(Debug, PartialEq, sqlx::Type, strum::AsRefStr)]
#[sqlx(type_name = "text")]
pub enum FunctionParallel {
    #[strum(serialize = "PARALLEL UNSAFE")]
    Unsafe,
    #[strum(serialize = "PARALLEL RESTRICTED")]
    Restricted,
    #[strum(serialize = "PARALLEL SAFE")]
    Safe,
}

macro_rules! impl_type_for_bool {
    ($e:ident, $default:ident) => {
        impl sqlx::Type<Postgres> for $e {
            fn type_info() -> PgTypeInfo {
                PgTypeInfo::with_name("bool")
            }
        }

        impl<'r> sqlx::Decode<'r, Postgres> for $e {
            fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
                if <bool as Decode<'r, Postgres>>::decode(value)? {
                    return Ok(Self::$default);
                }
                return Ok(Default::default());
            }
        }
    };
}

#[derive(Debug, Default, PartialEq, strum::AsRefStr)]
pub enum FunctionStrict {
    #[default]
    #[strum(serialize = "CALLED ON NULL INPUT")]
    Default,
    #[strum(serialize = "RETURNS NULL ON NULL INPUT")]
    Strict,
}

impl_type_for_bool!(FunctionStrict, Strict);

#[derive(Debug, Default, PartialEq, strum::AsRefStr)]
pub enum FunctionSecurity {
    #[default]
    #[strum(serialize = "SECURITY INVOKER")]
    Invoker,
    #[strum(serialize = "SECURITY DEFINER")]
    Definer,
}

impl_type_for_bool!(FunctionSecurity, Definer);
