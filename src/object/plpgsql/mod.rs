use lazy_regex::regex;
use pg_query::Error;

use serde::{Deserialize};
use serde_repr::Deserialize_repr;

use crate::object::{SchemaQualifiedName, BUILT_IN_FUNCTIONS, BUILT_IN_NAMES};
use crate::PgDiffError;

#[cfg(test)]
mod test;

/// Parse the pl/pgsql function declaration to 1 or more [PlPgSqlFunction]s
///
/// ## Errors
/// - If the function code cannot be parsed into an intermediary JSON structure
/// - if the JSON structure cannot be deserialized into an array of [PlPgSqlFunction]
pub fn parse_plpgsql_function(function_code: &str) -> Result<Vec<PlPgSqlFunction>, PgDiffError> {
    let parse_result = pg_query::parse_plpgsql(function_code).map_err(|error| {
        PgDiffError::General(format!("Could not parse plpg/sql function. {error}"))
    })?;
    serde_json::from_value(parse_result.clone())
        .map_err(|error| PgDiffError::General(format!("Could not parse plpg/sql ast. {error}\n{parse_result}")))
}

/// Trait to designate a type that can extract object name referenced within the node into a
/// supplied buffer
trait ObjectNode {
    /// Extract the names of referenced objects into the supplied `buffer`
    ///
    /// ## Errors
    /// There is only 1 case where an actual error can occur within the dependency fetching which is
    /// parsing an inner SQL expression.
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error>;
}

impl<O> ObjectNode for Vec<O>
where
    O: ObjectNode,
{
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        for node in self.iter() {
            node.extract_objects(buffer)?;
        }
        Ok(())
    }
}

impl<O> ObjectNode for Option<O>
where
    O: ObjectNode,
{
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        if let Some(node) = self {
            return node.extract_objects(buffer);
        }
        Ok(())
    }
}

/// SQL type name within a pl/pgsql block
#[derive(Debug, Deserialize)]
pub enum PlPgSqlType {
    #[serde(rename = "PLpgSQL_type")]
    Inner {
        #[serde(rename = "typname")]
        type_name: String,
    },
}

/// Field within a postgresql Row type
#[derive(Debug, Deserialize)]
pub struct RowField {
    name: String,
    #[serde(rename = "varno")]
    var_number: u32,
}

/// Variants of pl/pgsql variables types
#[derive(Debug, Deserialize)]
pub enum PlPgSqlVariable {
    /// Basic pl/pgsql variable
    #[serde(rename = "PLpgSQL_var")]
    Var {
        #[serde(rename = "datatype")]
        data_type: PlPgSqlType,
        #[serde(rename = "refname")]
        ref_name: String,
        #[serde(rename = "default_val", default)]
        default_value: Option<PlPgSqlExpr>,
        #[serde(rename = "lineno", default)]
        line_no: u32,
    },
    /// Postgresql row type variable. This a type when listing `INTO` variables from a SQL query
    #[serde(rename = "PLpgSQL_row")]
    Row {
        fields: Vec<RowField>,
        #[serde(rename = "lineno", default)]
        line_no: u32,
        #[serde(rename = "refname")]
        ref_name: String,
    },
    /// Pl/pgsql record variable. This is an opaque type with no concrete structure
    #[serde(rename = "PLpgSQL_rec")]
    Rec {
        dno: u32,
        #[serde(rename = "refname")]
        ref_name: String,
    },
    /// Pl/pgsql record field referenced as variable
    #[serde(rename = "PLpgSQL_recfield")]
    RecField {
        #[serde(rename = "fieldname")]
        field_name: String,
        #[serde(rename = "recparentno")]
        rec_parent_number: u32,
    },
}

impl ObjectNode for PlPgSqlVariable {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        let PlPgSqlVariable::Var {
            data_type,
            default_value,
            ..
        } = self
        else {
            return Ok(());
        };
        default_value.extract_objects(buffer)?;
        let PlPgSqlType::Inner { type_name } = data_type;
        buffer.push(SchemaQualifiedName::from(type_name));
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum RawParseMode {
    Default = 0,
    TypeName = 1,
    #[default]
    PlPgSqlExpr = 2,
    PlPgSqlAssign1 = 3,
    PlPgSqlAssign2 = 4,
    PlPgSqlAssign3 = 5,
}

/// Simple SQL expression within a pl/pgsql block
#[derive(Debug, Deserialize, PartialEq)]
pub enum PlPgSqlExpr {
    #[serde(rename = "PLpgSQL_expr")]
    Inner {
        #[serde(rename = "parseMode")]
        parse_mode: RawParseMode,
        query: String,
    },
}

impl ObjectNode for PlPgSqlExpr {
    /// Extract the objects referenced within this expression.
    ///
    /// These expressions can be regular DML statements, variable assignment or simple expressions
    /// such as `TRIM(x)`. If the statement is not a DML statement, the actual expression part will
    /// be prefixed with `select {expression}` to pass that expression to the SQL parser for
    /// evaluation. The result of the parsing is then checks for tables and function that are
    /// referenced.
    ///
    /// ## Errors
    /// If the SQL query parsing fails
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        let PlPgSqlExpr::Inner { query, .. } = self;
        let query = query.trim();
        let dml_query_regex = regex!("^select|insert|update|delete|truncate"i);
        let query = if dml_query_regex.is_match(query) {
            query.to_string()
        } else {
            match query.split_once(":=") {
                Some((_, assign_value)) => format!("select {assign_value}"),
                None => format!("select {query}"),
            }
        };
        let parse_result = pg_query::parse(&query)?;
        for table in parse_result.tables() {
            buffer.push(SchemaQualifiedName::from(&table));
        }
        for function in parse_result.functions() {
            if BUILT_IN_FUNCTIONS.contains(&function.as_str()) {
                continue;
            }
            buffer.push(SchemaQualifiedName::from(&function));
        }
        Ok(())
    }
}

/// `ELSIF` block within an `IF` structure
#[derive(Debug, Deserialize)]
pub enum PlPgSqlElsIf {
    #[serde(rename = "PLpgSQL_if_elsif")]
    Inner {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Condition checked to enter the `ELSIF` block
        #[serde(rename = "cond")]
        condition: PlPgSqlExpr,
        /// Statements executed if the condition returns true
        #[serde(rename = "stmts")]
        statements: Vec<PlPgSqlStatement>,
    },
}

impl ObjectNode for PlPgSqlElsIf {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        let PlPgSqlElsIf::Inner {
            condition,
            statements,
            ..
        } = self;
        condition.extract_objects(buffer)?;
        statements.extract_objects(buffer)?;
        Ok(())
    }
}

/// `WHEN` block of a `CASE` statement
#[derive(Debug, Deserialize)]
pub struct PlPgSqlCaseWhen {
    #[serde(rename = "lineno")]
    line_no: u32,
    /// Expression checked to enter the `WHEN` block
    #[serde(rename = "expr")]
    expression: PlPgSqlExpr,
    /// Statement executed if the expression returns true
    #[serde(rename = "stmts")]
    statements: Vec<PlPgSqlStatement>,
}

impl ObjectNode for PlPgSqlCaseWhen {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        self.expression.extract_objects(buffer)?;
        self.statements.extract_objects(buffer)?;
        Ok(())
    }
}

/// Variants of cursor open statements
#[derive(Debug, Deserialize)]
pub enum PlPgSqlOpenCursor {
    /// Simple query used to open the cursor. No parameters are accepted.
    Query { query: PlPgSqlExpr },
    /// Dynamic expression executed with a variable number of parameters supplied.
    Execute {
        #[serde(rename = "dynquery")]
        dyn_query: PlPgSqlExpr,
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    /// Executes a previously prepared cursor with the argument supplied
    Args {
        #[serde(rename = "argquery")]
        query_args: PlPgSqlExpr,
    },
}

impl ObjectNode for PlPgSqlOpenCursor {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        match self {
            PlPgSqlOpenCursor::Query { query } => {
                query.extract_objects(buffer)?;
            }
            PlPgSqlOpenCursor::Execute { dyn_query, params } => {
                dyn_query.extract_objects(buffer)?;
                params.extract_objects(buffer)?;
            }
            PlPgSqlOpenCursor::Args { query_args } => {
                query_args.extract_objects(buffer)?;
            }
        }
        Ok(())
    }
}

/// Fetch direction for a cursor
#[derive(Debug, Deserialize)]
pub enum FetchDirection {
    Forward,
    Backward,
    Absolute,
    Relative,
}

/// Pl/pgsql exception condition name (e.g. `NO_DATA_FOUND`)
#[derive(Debug, Deserialize)]
pub enum PlPgSqlExceptionCondition {
    #[serde(rename = "PLpgSQL_condition")]
    Inner {
        #[serde(rename = "condname")]
        condition_name: String,
    },
}

/// Pl/pgsql exception catch block. Handles 1 or more conditions and executes the action statements
#[derive(Debug, Deserialize)]
pub enum PlPgSqlException {
    #[serde(rename = "PLpgSQL_exception")]
    Inner {
        #[serde(rename = "lineno")]
        line_no: u32,
        conditions: Vec<PlPgSqlExceptionCondition>,
        action: Vec<PlPgSqlStatement>,
    },
}

impl ObjectNode for PlPgSqlException {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        let PlPgSqlException::Inner { action, .. } = self;
        action.extract_objects(buffer)?;
        Ok(())
    }
}

/// Diagnostic details to fetch as part of a `GET DIAGNOSTICS` statement
#[derive(Debug, Deserialize)]
pub enum PlPgSqlDiagnoticsKind {
    #[serde(rename = "ROW_COUNT")]
    RowCount,
    #[serde(rename = "PG_ROUTINE_OID")]
    RoutineOid,
    #[serde(rename = "PG_CONTEXT")]
    Context,
    #[serde(rename = "PG_EXCEPTION_CONTEXT")]
    ErrorContext,
    #[serde(rename = "PG_EXCEPTION_DETAIL")]
    ErrorDetails,
    #[serde(rename = "PG_EXCEPTION_HINT")]
    ErrorHint,
    #[serde(rename = "RETURNED_SQLSTATE")]
    ReturnedSqlState,
    #[serde(rename = "COLUMN_NAME")]
    ColumnName,
    #[serde(rename = "CONSTRAINT_NAME")]
    ConstraintName,
    #[serde(rename = "PG_DATATYPE_NAME")]
    DataTypeName,
    #[serde(rename = "MESSAGE_TEXT")]
    MessageText,
    #[serde(rename = "TABLE_NAME")]
    TableName,
    #[serde(rename = "SCHEMA_NAME")]
    SchemaName,
}

/// `GET DIAGNOSTICS` statement item
#[derive(Debug, Deserialize)]
pub enum PlPgSqlDiagnosticsItem {
    #[serde(rename = "PLpgSQL_diag_item")]
    Inner {
        kind: PlPgSqlDiagnoticsKind,
        target: u32,
    },
}

/// Exceptions catch block where 1 or more exceptions are caught and actions are performed
#[derive(Debug, Deserialize)]
pub enum PlPgSqlExceptionBlock {
    #[serde(rename = "PLpgSQL_exception_block")]
    Inner {
        #[serde(rename = "sqlstate_varno", default)]
        sql_state_variable_no: u32,
        #[serde(rename = "sqlerrm_varno", default)]
        sql_error_variable_no: u32,
        #[serde(rename = "exc_list", default)]
        exceptions: Vec<PlPgSqlException>,
    },
}

impl ObjectNode for PlPgSqlExceptionBlock {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        let PlPgSqlExceptionBlock::Inner { exceptions, .. } = self;
        exceptions.extract_objects(buffer)?;
        Ok(())
    }
}

/// Raise statement level variants as integer codes
#[derive(Debug, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum PlPgSqlRaiseLogLevel {
    Error = 21,
    Warning = 19,
    Notice = 18,
    Info = 17,
    Log = 15,
    Debug = 14,
}

/// Cursor options as integers
#[derive(Debug, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum CursorOption {
    None = 0,
    Scroll = 2,
    NoScroll = 4,
}

/// All pl/pgsql statement types as defined by `pg_query`
#[derive(Debug, Deserialize)]
pub enum PlPgSqlStatement {
    /// Generic pl/pgsql block which is wrapped in `BEGIN; ... END;`
    #[serde(rename = "PLpgSQL_stmt_block")]
    Block {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional label for the block
        #[serde(default)]
        label: Option<String>,
        /// 1 or more statements executed within the body of the block. Can be
        /// [PlPgSqlStatement::Block]s themselves
        body: Vec<PlPgSqlStatement>,
        /// Number of variables declared at the start of the block
        #[serde(rename = "n_initvars", default)]
        declare_var_count: u32,
        /// Variables numbers (if any)
        #[serde(rename = "initvarnos", default)]
        declare_var_numbers: Option<Vec<u32>>,
        /// Optional catch block for exceptions
        #[serde(default)]
        exceptions: Option<PlPgSqlExceptionBlock>,
    },
    /// Assignment statement for a variable
    #[serde(rename = "PLpgSQL_stmt_assign")]
    Assign {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "varno")]
        var_number: u32,
        /// Full expression of the assignment including the variable name and assignment operator
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
    },
    /// Full `IF` statement including the required `THEN` block as well as the optional `ELSIF` and
    /// `ELSE` blocks
    #[serde(rename = "PLpgSQL_stmt_if")]
    If {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Condition that must be true for the `then_body` to be executed
        #[serde(rename = "cond")]
        condition: PlPgSqlExpr,
        /// 1 or more statements executed if the `condition` is true
        then_body: Vec<PlPgSqlStatement>,
        /// Optional `ELSIF` blocks
        #[serde(rename = "elsif_list", default)]
        elsif_body: Option<Vec<PlPgSqlElsIf>>,
        /// Optional `ELSE` block as zero or more statements
        #[serde(default)]
        else_body: Option<Vec<PlPgSqlStatement>>,
    },
    /// Full `CASE` block including `WHEN` and `ELSE` branches
    #[serde(rename = "PLpgSQL_stmt_case")]
    Case {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional expression tested in every `WHEN` branch
        #[serde(rename = "t_expr")]
        test_expression: Option<PlPgSqlExpr>,
        #[serde(rename = "t_varno", default)]
        test_var_number: u32,
        /// 1 or more `WHEN` branches of the `CASE` statement
        #[serde(rename = "case_when_list")]
        whens: Vec<PlPgSqlCaseWhen>,
        /// Optional `ELSE` blocks containing zero or more statements
        #[serde(rename = "else_stmts", default)]
        else_statements: Option<Vec<PlPgSqlStatement>>,
    },
    /// Simple `LOOP` block
    #[serde(rename = "PLpgSQL_stmt_loop")]
    Loop {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional label for operations against a named loop
        #[serde(default)]
        label: Option<String>,
        /// 1 or more statements executed within the loop block
        body: Vec<PlPgSqlStatement>,
    },
    /// `WHILE` loop block
    #[serde(rename = "PLpgSQL_stmt_while")]
    While {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional label for operations against a named loop
        #[serde(default)]
        label: Option<String>,
        /// Condition tested on each iteration to set an automated break condition
        #[serde(rename = "cond")]
        condition: PlPgSqlExpr,
        /// 1 or more statements executed within the loop block
        body: Vec<PlPgSqlStatement>,
    },
    /// `FOR` loop over an integer range as bounds
    #[serde(rename = "PLpgSQL_stmt_fori")]
    ForI {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional label for operations against a named loop
        #[serde(default)]
        label: Option<String>,
        /// Variable for integer used in iteration
        var: PlPgSqlVariable,
        /// Lower bound of the loop
        lower: PlPgSqlExpr,
        /// Update bound of the loop
        upper: PlPgSqlExpr,
        /// Optional step specified with `BY expression`. [None] means default of 1
        #[serde(default)]
        step: Option<PlPgSqlExpr>,
        /// True if the `REVERSE` keyword is used to perform the loop in the reverse order. False
        /// means regular ascending order.
        #[serde(default)]
        reverse: bool,
        /// 1 or more statements executed within the loop block
        body: Vec<PlPgSqlStatement>,
    },
    /// `FOR` loop over a static SQL query is executed
    #[serde(rename = "PLpgSQL_stmt_fors")]
    ForS {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional label for operations against a named loop
        #[serde(default)]
        label: Option<String>,
        /// Variable used to store each row of the query returned
        var: PlPgSqlVariable,
        /// 1 or more statements executed within the loop block
        body: Vec<PlPgSqlStatement>,
        /// SQL query executed to generate the loop rows
        query: PlPgSqlExpr,
    },
    /// `FOR` loop over a cursor's results
    #[serde(rename = "PLpgSQL_stmt_forc")]
    ForC {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional label for operations against a named loop
        #[serde(default)]
        label: Option<String>,
        /// Variable used to store each row of the cursor returned
        var: PlPgSqlVariable,
        /// 1 or more statements executed within the loop block
        body: Vec<PlPgSqlStatement>,
        cursor_var: u32,
        /// Optional cursor arguments supplied before executing the cursor. This will always be
        /// a list of assignment expressions
        #[serde(rename = "argquery", default)]
        query_args: Option<Vec<PlPgSqlExpr>>,
    },
    /// `FOREACH` loop over an array
    #[serde(rename = "PLpgSQL_stmt_foreach_a")]
    Foreach {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "varno")]
        var_number: u32,
        #[serde(rename = "slice", default)]
        slice_dimension: u32,
        /// Expression that either points to an array variable or evaluates to an array
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
        /// 1 or more statements executed within the loop block
        body: Vec<PlPgSqlStatement>,
    },
    /// `EXIT` or `CONTINUE` statements within a loop. They are grouped together because the syntax
    /// is the same, they just perform different actions within the loop. The 2 options are
    /// distinguished by the `is_exit` field.
    #[serde(rename = "PLpgSQL_stmt_exit")]
    ExitOrContinue {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// True if this is an `EXIT` statement. Otherwise, it's a `CONTINUE` statement.
        #[serde(default)]
        is_exit: bool,
        /// Optional label for operations against a named loop
        #[serde(default)]
        label: Option<String>,
        /// Optional boolean condition to check before performing the `EXIT`/`CONTINUE`
        #[serde(rename = "cond", default)]
        condition: Option<PlPgSqlExpr>,
    },
    /// `RETURN` statement
    #[serde(rename = "PLpgSQL_stmt_return")]
    Return {
        #[serde(rename = "lineno", default)]
        line_no: u32,
        /// Optional expression returned from the function. If [None] the function returns void or
        /// this signifies the end of a set returning function.
        #[serde(rename = "expr")]
        expression: Option<PlPgSqlExpr>,
        #[serde(rename = "retvarno", default)]
        return_var_no: i32,
    },
    /// `RETURN NEXT` statement to generate the next item returned from a set returning function
    #[serde(rename = "PLpgSQL_stmt_return_next")]
    ReturnNext {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Expression executed to generate the next return value
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
        #[serde(rename = "retvarno", default)]
        return_var_no: i32,
    },
    /// `RETURN QUERY` statement to generate a batch of items returned from a set returning function
    #[serde(rename = "PLpgSQL_stmt_return_query")]
    ReturnQuery {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// [Some] if this is a static query executed
        #[serde(rename = "query")]
        query: Option<PlPgSqlExpr>,
        /// [Some] if this is a dynamic query string executed
        #[serde(rename = "dynquery")]
        dynamic_query: Option<PlPgSqlExpr>,
        /// Optional list of arguments supplied to a dynamic query
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    /// `RAISE` statement to raise a notice/error within a pl/pgsql block
    #[serde(rename = "PLpgSQL_stmt_raise")]
    Raise {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Level of the raise message
        #[serde(rename = "elog_level")]
        error_log_level: PlPgSqlRaiseLogLevel,
        /// Optional condition name specified as a known pl/pgsql error
        #[serde(rename = "condname", default)]
        condition_name: Option<String>,
        /// Optional message included with the raise
        #[serde(default)]
        message: Option<String>,
        /// Option parameters supplied to the custom raise message
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
        /// zero or more options included with the raise
        #[serde(default)]
        options: Option<Vec<PlPgSqlRaiseOption>>,
    },
    /// Simple `ASSERT` statement to check a condition is truthy
    #[serde(rename = "PLpgSQL_stmt_assert")]
    Assert {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Condition checked as a boolean expression
        #[serde(rename = "cond")]
        condition: PlPgSqlExpr,
        /// Optional message included in the assertion error if the `condition` is false
        #[serde(default)]
        message: Option<PlPgSqlExpr>,
    },
    /// Execute a static SQL command
    #[serde(rename = "PLpgSQL_stmt_execsql")]
    ExecSql {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// SQL query executed
        #[serde(rename = "sqlstmt")]
        sql_statement: PlPgSqlExpr,
        /// True if the statement is deemed to be statement that modifies data
        #[serde(rename = "mod_stmt", default)]
        is_dml_statement: bool,
        /// True if the statement putting resulting values into a variable (or multiple variables)
        #[serde(default)]
        into: bool,
        /// True if the query should fail if a row is required to be returned. False will set the
        /// variable(s) to null
        #[serde(rename = "strict", default)]
        is_strict: bool,
        /// Optional target variable(s) to put the resulting row into. [Some] when `into` is true.
        #[serde(default)]
        target: Option<PlPgSqlVariable>,
    },
    /// Execute a dynamic SQL query using `EXECUTE {query_string}`
    #[serde(rename = "PLpgSQL_stmt_dynexecute")]
    DynExecute {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// SQL query executed
        query: PlPgSqlExpr,
        /// True if the statement putting resulting values into a variable (or multiple variables)
        #[serde(default)]
        into: bool,
        /// True if the query should fail if a row is required to be returned. False will set the
        /// variable(s) to null
        #[serde(rename = "strict", default)]
        is_strict: bool,
        /// Optional target variable(s) to put the resulting row into. [Some] when `into` is true.
        #[serde(default)]
        target: Option<PlPgSqlVariable>,
        /// Optional parameters supplied to the dynamic query when it's parameterized
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    /// `FOR` loop over a dynamic query
    #[serde(rename = "PLpgSQL_stmt_dynfors")]
    DynForS {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Optional label for operations against a named loop
        #[serde(default)]
        label: Option<String>,
        /// Variable used to store each row of the query returned
        var: PlPgSqlVariable,
        /// 1 or more statements executed within the loop block
        body: Vec<PlPgSqlStatement>,
        /// SQL query executed to generate the loop rows
        query: PlPgSqlExpr,
        /// Zero or more parameters supplied to the dynamic query
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    /// `GET DIAGNOTICS` statement
    #[serde(rename = "PLpgSQL_stmt_getdiag")]
    GetDiagnostics {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// True if `STACKED` keyword is present
        is_stacked: bool,
        /// 1 or more diagnostics items present
        #[serde(rename = "diag_items")]
        diagnostics_items: Vec<PlPgSqlDiagnosticsItem>,
    },
    /// `OPEN` cursor statement
    #[serde(rename = "PLpgSQL_stmt_open")]
    Open {
        #[serde(rename = "lineno")]
        line_no: u32,
        cursor_var: u32,
        cursor_options: CursorOption,
        /// Cursor open query options
        #[serde(flatten)]
        query: PlPgSqlOpenCursor,
    },
    /// `FETCH`/`MOVE` for cursor statement
    #[serde(rename = "PLpgSQL_stmt_fetch")]
    Fetch {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// Variable(s) used to store the operation results if `FETCH` statement
        target: Option<PlPgSqlVariable>,
        #[serde(rename = "curvar")]
        cursor_variable: u32,
        /// Fetch direction variant
        direction: FetchDirection,
        /// Static number of records fetched if `direction` is [FetchDirection::Absolute] or
        /// [FetchDirection::Relative]
        #[serde(rename = "how_many", default)]
        fetch_count: Option<u64>,
        /// Computed number of records fetched if `direction` is [FetchDirection::Absolute] or
        /// [FetchDirection::Relative]
        #[serde(rename = "expr", default)]
        fetch_count_expr: Option<PlPgSqlExpr>,
        /// True if the statement is `MOVE`. Otherwise `FETCH`.
        #[serde(default)]
        is_move: bool,
        /// True if the fetch count is > 1
        #[serde(default)]
        returns_multiple_rows: bool,
    },
    /// `CLOSE` cursor statement
    Close {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "curvar")]
        cursor_variable: u32,
    },
    /// `PERFORM` statement with SQL query, returning no actual data
    #[serde(rename = "PLpgSQL_stmt_perform")]
    Perform {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// SQL query to execute. Any result is ignored.
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
    },
    /// `CALL`/`DO` statement
    #[serde(rename = "PLpgSQL_stmt_call")]
    Call {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// SQL expression executed as part of the statement execution
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
        /// True if `CALL` statement. Otherwise, it's a `DO` statement.
        #[serde(default)]
        is_call: bool,
    },
    /// `COMMIT` statement
    #[serde(rename = "PLpgSQL_stmt_commit")]
    Commit {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// True if the `AND CHAIN` modifier to the `COMMIT` are present
        chain: bool,
    },
    /// `ROLLBACK` statement
    #[serde(rename = "PLpgSQL_stmt_rollback")]
    Rollback {
        #[serde(rename = "lineno")]
        line_no: u32,
        /// True if the `AND CHAIN` modifier to the `ROLLBACK` are present
        chain: bool,
    },
}

/// `RAISE` statement option as a key value pair. The key is a known option type and the value is
/// an SQL expression.
#[derive(Debug, Deserialize, PartialEq)]
pub enum PlPgSqlRaiseOption {
    #[serde(rename = "PLpgSQL_raise_option")]
    Inner {
        #[serde(rename = "opt_type")]
        option_type: PlPgSqlRaiseOptionType,
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
    },
}

impl ObjectNode for PlPgSqlRaiseOption {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        let PlPgSqlRaiseOption::Inner { expression, .. } = self;
        expression.extract_objects(buffer)?;
        Ok(())
    }
}

/// Variant of option names supplied to a pl/pgsql `RAISE` statement
#[derive(Debug, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum PlPgSqlRaiseOptionType {
    ErrorCode = 0,
    Message,
    Detail,
    Hint,
    Column,
    Constraint,
    DataType,
    Table,
    Schema,
}

impl ObjectNode for PlPgSqlStatement {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), Error> {
        match self {
            PlPgSqlStatement::Block {
                body, exceptions, ..
            } => {
                body.extract_objects(buffer)?;
                exceptions.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Assign { expression, .. } => {
                expression.extract_objects(buffer)?;
            }
            PlPgSqlStatement::If {
                condition,
                then_body,
                elsif_body,
                else_body,
                ..
            } => {
                condition.extract_objects(buffer)?;
                then_body.extract_objects(buffer)?;
                elsif_body.extract_objects(buffer)?;
                else_body.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Case {
                test_expression,
                whens,
                else_statements,
                ..
            } => {
                test_expression.extract_objects(buffer)?;
                whens.extract_objects(buffer)?;
                else_statements.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Loop { body, .. } => {
                body.extract_objects(buffer)?;
            }
            PlPgSqlStatement::While { body, .. } => {
                body.extract_objects(buffer)?;
            }
            PlPgSqlStatement::ForI {
                var,
                lower,
                upper,
                step,
                body,
                ..
            } => {
                var.extract_objects(buffer)?;
                lower.extract_objects(buffer)?;
                upper.extract_objects(buffer)?;
                step.extract_objects(buffer)?;
                body.extract_objects(buffer)?;
            }
            PlPgSqlStatement::ForS {
                var, body, query, ..
            } => {
                var.extract_objects(buffer)?;
                body.extract_objects(buffer)?;
                query.extract_objects(buffer)?;
            }
            PlPgSqlStatement::ForC {
                var,
                body,
                query_args,
                ..
            } => {
                var.extract_objects(buffer)?;
                body.extract_objects(buffer)?;
                query_args.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Foreach {
                expression, body, ..
            } => {
                expression.extract_objects(buffer)?;
                body.extract_objects(buffer)?;
            }
            PlPgSqlStatement::ExitOrContinue { condition, .. } => {
                condition.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Return { expression, .. } => {
                expression.extract_objects(buffer)?;
            }
            PlPgSqlStatement::ReturnNext { expression, .. } => {
                expression.extract_objects(buffer)?;
            }
            PlPgSqlStatement::ReturnQuery {
                query,
                dynamic_query,
                params,
                ..
            } => {
                query.extract_objects(buffer)?;
                dynamic_query.extract_objects(buffer)?;
                params.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Raise {
                params, options, ..
            } => {
                params.extract_objects(buffer)?;
                options.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Assert { condition, .. } => {
                condition.extract_objects(buffer)?;
            }
            PlPgSqlStatement::ExecSql {
                sql_statement,
                target,
                ..
            } => {
                sql_statement.extract_objects(buffer)?;
                target.extract_objects(buffer)?;
            }
            PlPgSqlStatement::DynExecute {
                query,
                target,
                params,
                ..
            } => {
                query.extract_objects(buffer)?;
                target.extract_objects(buffer)?;
                params.extract_objects(buffer)?;
            }
            PlPgSqlStatement::DynForS {
                body,
                query,
                params,
                ..
            } => {
                body.extract_objects(buffer)?;
                query.extract_objects(buffer)?;
                params.extract_objects(buffer)?;
            }
            PlPgSqlStatement::GetDiagnostics { .. } => {}
            PlPgSqlStatement::Open { query, .. } => {
                query.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Fetch {
                target,
                fetch_count_expr,
                ..
            } => {
                target.extract_objects(buffer)?;
                fetch_count_expr.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Close { .. } => {}
            PlPgSqlStatement::Perform { expression, .. } => {
                expression.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Call {
                expression, ..
            } => {
                expression.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Commit { .. } => {}
            PlPgSqlStatement::Rollback { .. } => {}
        }
        Ok(())
    }
}

/// Pl/pgsql sql function declaration block
#[derive(Debug, Deserialize)]
pub enum PlPgSqlFunction {
    #[serde(rename = "PLpgSQL_function")]
    Inner {
        /// Variable number for the `NEW` variable available to trigger function. 0 when not a
        /// trigger function.
        #[serde(rename = "new_varno", default)]
        new_variable_no: u32,
        /// Variable number for the `OLD` variable available to trigger function. 0 when not a
        /// trigger function.
        #[serde(rename = "old_varno", default)]
        old_variable_no: u32,
        /// Data types and variables known to be part of the function
        #[serde(default)]
        datums: Vec<PlPgSqlVariable>,
        /// Main action of the function. This should always be [PlPgSqlStatement::Block].
        action: PlPgSqlStatement,
    },
}

impl PlPgSqlFunction {
    pub fn get_objects(&self) -> Result<Vec<SchemaQualifiedName>, PgDiffError> {
        let PlPgSqlFunction::Inner { action, datums, .. } = self;
        let mut result = vec![];
        action
            .extract_objects(&mut result)
            .map_err(|error| PgDiffError::PgQuery {
                object_name: SchemaQualifiedName::from("pl_pgsql_block"),
                error,
            })?;
        for datum in datums {
            if let PlPgSqlVariable::Var {
                data_type: PlPgSqlType::Inner { type_name },
                ..
            } = datum
            {
                if type_name == "UNKNOWN" || BUILT_IN_NAMES.contains(&type_name.as_str()) {
                    continue;
                }
                result.push(SchemaQualifiedName::from(type_name))
            }
        }
        Ok(result)
    }
}
