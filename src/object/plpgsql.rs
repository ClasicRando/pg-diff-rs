use lazy_regex::regex;
use std::str::FromStr;

use serde::Deserialize;

use crate::object::SchemaQualifiedName;
use crate::PgDiffError;

trait ObjectNode {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError>;
}

impl<O> ObjectNode for Vec<O>
where
    O: ObjectNode,
{
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
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
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
        if let Some(node) = self {
            return node.extract_objects(buffer);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlType {
    #[serde(rename = "PLpgSQL_type")]
    Inner {
        #[serde(rename = "typname")]
        type_name: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct RowField {
    name: String,
    #[serde(rename = "varno")]
    var_number: u32,
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlVariable {
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
    #[serde(rename = "PLpgSQL_row")]
    Row {
        fields: Vec<RowField>,
        #[serde(rename = "lineno", default)]
        line_no: u32,
        #[serde(rename = "refname")]
        ref_name: String,
    },
    #[serde(rename = "PLpgSQL_rec")]
    Rec {
        dno: u32,
        #[serde(rename = "refname")]
        ref_name: String,
    },
    #[serde(rename = "PLpgSQL_recfield")]
    RecField {
        #[serde(rename = "fieldname")]
        field_name: String,
        #[serde(rename = "recparentno")]
        rec_parent_number: u32,
    },
    // Promise,
}

impl ObjectNode for PlPgSqlVariable {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
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
        buffer.push(SchemaQualifiedName::from_str(type_name)?);
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlExpr {
    #[serde(rename = "PLpgSQL_expr")]
    Inner {
        #[serde(rename = "parseMode")]
        parse_mode: i32,
        query: String,
    },
}

impl ObjectNode for PlPgSqlExpr {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
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
        let parse_result = match pg_query::parse(&query) {
            Ok(inner) => inner,
            Err(error) => {
                return Err(PgDiffError::PgQuery {
                    object_name: SchemaQualifiedName::from_str("plpgsql_block")?,
                    error,
                })
            }
        };
        for table in parse_result.tables() {
            buffer.push(SchemaQualifiedName::from_str(&table)?);
        }
        for function in parse_result.functions() {
            buffer.push(SchemaQualifiedName::from_str(&function)?);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlElsIf {
    #[serde(rename = "PLpgSQL_if_elsif")]
    Inner {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "cond")]
        condition: PlPgSqlExpr,
        #[serde(rename = "stmts")]
        statements: Vec<PlPgSqlStatement>,
    },
}

impl ObjectNode for PlPgSqlElsIf {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
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

#[derive(Debug, Deserialize)]
pub struct PlPgSqlCaseWhen {
    #[serde(rename = "lineno")]
    line_no: u32,
    #[serde(rename = "expr")]
    expression: PlPgSqlExpr,
    #[serde(rename = "stmts")]
    statements: Vec<PlPgSqlStatement>,
}

impl ObjectNode for PlPgSqlCaseWhen {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
        self.expression.extract_objects(buffer)?;
        self.statements.extract_objects(buffer)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlOpenCursor {
    Query {
        query: PlPgSqlExpr,
    },
    Execute {
        #[serde(rename = "dynquery")]
        dyn_query: PlPgSqlExpr,
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    Args {
        #[serde(rename = "argquery")]
        query_args: PlPgSqlExpr,
    },
}

impl ObjectNode for PlPgSqlOpenCursor {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
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

#[derive(Debug, Deserialize)]
pub enum FetchDirection {
    Forward,
    Backward,
    Absolute,
    Relative,
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlExceptionCondition {
    #[serde(rename = "PLpgSQL_condition")]
    Inner {
        #[serde(rename = "condname")]
        condition_name: String,
    },
}

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
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
        let PlPgSqlException::Inner { action, .. } = self;
        action.extract_objects(buffer)?;
        Ok(())
    }
}

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

#[derive(Debug, Deserialize)]
pub enum PlPgSqlDiagnosticsItem {
    #[serde(rename = "PLpgSQL_diag_item")]
    Inner {
        kind: PlPgSqlDiagnoticsKind,
        target: u32,
    },
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlExceptionBlock {
    #[serde(rename = "PLpgSQL_exception_block")]
    Inner {
        #[serde(rename = "argquery", default)]
        sql_state_variable_no: u32,
        #[serde(rename = "argquery", default)]
        sql_error_variable_no: u32,
        #[serde(rename = "exc_list", default)]
        exceptions: Option<Vec<PlPgSqlException>>,
    },
}

impl ObjectNode for PlPgSqlExceptionBlock {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
        let PlPgSqlExceptionBlock::Inner { exceptions, .. } = self;
        exceptions.extract_objects(buffer)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlStatement {
    #[serde(rename = "PLpgSQL_stmt_block")]
    Block {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(default)]
        label: Option<String>,
        body: Vec<PlPgSqlStatement>,
        #[serde(rename = "n_initvars", default)]
        declare_var_count: u32,
        #[serde(rename = "initvarnos", default)]
        declare_var_numbers: Option<Vec<u32>>,
        #[serde(default)]
        exceptions: Option<PlPgSqlExceptionBlock>,
    },
    #[serde(rename = "PLpgSQL_stmt_assign")]
    Assign {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "varno")]
        var_number: u32,
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
    },
    #[serde(rename = "PLpgSQL_stmt_if")]
    If {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "cond")]
        condition: PlPgSqlExpr,
        then_body: Vec<PlPgSqlStatement>,
        #[serde(default)]
        elsif_body: Option<Vec<PlPgSqlElsIf>>,
        #[serde(default)]
        else_body: Option<Vec<PlPgSqlStatement>>,
    },
    #[serde(rename = "PLpgSQL_stmt_case")]
    Case {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "t_expr")]
        test_expression: Option<PlPgSqlExpr>,
        #[serde(rename = "t_varno", default)]
        test_var_number: u32,
        #[serde(rename = "case_when_list")]
        whens: Vec<PlPgSqlCaseWhen>,
        #[serde(rename = "else_stmts", default)]
        else_statements: Option<Vec<PlPgSqlStatement>>,
    },
    #[serde(rename = "PLpgSQL_stmt_loop")]
    Loop {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(default)]
        label: Option<String>,
        body: Vec<PlPgSqlStatement>,
    },
    #[serde(rename = "PLpgSQL_stmt_while")]
    While {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(default)]
        label: Option<String>,
        #[serde(rename = "cond")]
        condition: String,
        body: Vec<PlPgSqlStatement>,
    },
    #[serde(rename = "PLpgSQL_stmt_fori")]
    ForI {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(default)]
        label: Option<String>,
        var: PlPgSqlVariable,
        lower: PlPgSqlExpr,
        upper: PlPgSqlExpr,
        #[serde(default)]
        step: Option<PlPgSqlExpr>,
        #[serde(default)]
        reverse: i32,
        body: Vec<PlPgSqlStatement>,
    },
    #[serde(rename = "PLpgSQL_stmt_fors")]
    ForS {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(default)]
        label: Option<String>,
        var: PlPgSqlVariable,
        body: Vec<PlPgSqlStatement>,
        query: PlPgSqlExpr,
    },
    #[serde(rename = "PLpgSQL_stmt_forc")]
    ForC {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(default)]
        label: Option<String>,
        var: PlPgSqlVariable,
        body: Vec<PlPgSqlStatement>,
        cursor_var: u32,
        #[serde(rename = "argquery", default)]
        query_args: Option<PlPgSqlExpr>,
    },
    #[serde(rename = "PLpgSQL_stmt_foreach_a")]
    Foreach {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "varno")]
        var_number: u32,
        #[serde(rename = "slice", default)]
        slice_dimension: u32,
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
        body: Vec<PlPgSqlStatement>,
    },
    #[serde(rename = "PLpgSQL_stmt_exit")]
    ExitOrContinue {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(default)]
        is_exit: bool,
        #[serde(default)]
        label: Option<String>,
        #[serde(rename = "cond", default)]
        condition: Option<PlPgSqlExpr>,
    },
    #[serde(rename = "PLpgSQL_stmt_return")]
    Return {
        #[serde(rename = "lineno", default)]
        line_no: u32,
        #[serde(rename = "expr")]
        expression: Option<PlPgSqlExpr>,
        #[serde(rename = "retvarno", default)]
        return_var_no: i32,
    },
    #[serde(rename = "PLpgSQL_stmt_return_next")]
    ReturnNext {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
        #[serde(rename = "retvarno", default)]
        return_var_no: i32,
    },
    #[serde(rename = "PLpgSQL_stmt_return_query")]
    ReturnQuery {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "query")]
        query: Option<PlPgSqlExpr>,
        #[serde(rename = "dynquery")]
        dynamic_query: Option<PlPgSqlExpr>,
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    #[serde(rename = "PLpgSQL_stmt_raise")]
    Raise {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "elog_level")]
        error_log_level: i32,
        #[serde(rename = "condname", default)]
        condition_name: Option<String>,
        #[serde(default)]
        message: Option<String>,
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
        #[serde(default)]
        options: Option<Vec<PlPgSqlRaiseOption>>,
    },
    #[serde(rename = "PLpgSQL_stmt_assert")]
    Assert {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "cond", default)]
        condition: Option<PlPgSqlExpr>,
        #[serde(default)]
        message: Option<String>,
    },
    #[serde(rename = "PLpgSQL_stmt_execsql")]
    ExecSql {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "sqlstmt")]
        sql_statement: PlPgSqlExpr,
        #[serde(rename = "mod_stmt", default)]
        is_dml_statement: bool,
        #[serde(default)]
        into: bool,
        #[serde(rename = "strict", default)]
        is_strict: bool,
        #[serde(default)]
        target: Option<PlPgSqlVariable>,
    },
    #[serde(rename = "PLpgSQL_stmt_dynexecute")]
    DynExecute {
        #[serde(rename = "lineno")]
        line_no: u32,
        query: PlPgSqlExpr,
        #[serde(default)]
        into: bool,
        #[serde(rename = "strict", default)]
        is_strict: bool,
        #[serde(default)]
        target: Option<PlPgSqlVariable>,
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    #[serde(rename = "PLpgSQL_stmt_dynfors")]
    DynForS {
        #[serde(rename = "lineno")]
        line_no: u32,
        label: Option<String>,
        #[serde(rename = "var")]
        loop_variable: PlPgSqlVariable,
        body: Vec<PlPgSqlStatement>,
        query: PlPgSqlExpr,
        #[serde(default)]
        params: Option<Vec<PlPgSqlExpr>>,
    },
    #[serde(rename = "PLpgSQL_stmt_getdiag")]
    GetDiagnostics {
        #[serde(rename = "lineno")]
        line_no: u32,
        is_stacked: bool,
        #[serde(rename = "diag_items")]
        diagnotics_items: Vec<PlPgSqlDiagnosticsItem>,
    },
    #[serde(rename = "PLpgSQL_stmt_open")]
    Open {
        #[serde(rename = "lineno")]
        line_no: u32,
        cursor_var: u32,
        cursor_options: i32,
        #[serde(flatten)]
        query: PlPgSqlOpenCursor,
    },
    #[serde(rename = "PLpgSQL_stmt_fetch")]
    Fetch {
        #[serde(rename = "lineno")]
        line_no: u32,
        target: PlPgSqlVariable,
        #[serde(rename = "curvar")]
        cursor_variable: u32,
        direction: FetchDirection,
        #[serde(rename = "how_many", default)]
        fetch_count: Option<u64>,
        #[serde(rename = "expr", default)]
        fetch_count_expr: Option<PlPgSqlExpr>,
        #[serde(default)]
        is_move: bool,
        #[serde(default)]
        returns_multiple_rows: bool,
    },
    Close {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "curvar")]
        cursor_variable: u32,
    },
    #[serde(rename = "PLpgSQL_stmt_perform")]
    Perform {
        #[serde(rename = "lineno")]
        line_no: u32,
        #[serde(rename = "expr")]
        expression: PlPgSqlExpr,
    },
    #[serde(rename = "PLpgSQL_stmt_call")]
    Call {
        #[serde(rename = "lineno")]
        line_no: u32,
        expression: PlPgSqlExpr,
        is_call: bool,
        #[serde(rename = "target")]
        target: Option<PlPgSqlVariable>,
    },
    #[serde(rename = "PLpgSQL_stmt_commit")]
    Commit {
        #[serde(rename = "lineno")]
        line_no: u32,
        chain: bool,
    },
    #[serde(rename = "PLpgSQL_stmt_rollback")]
    Rollback {
        #[serde(rename = "lineno")]
        line_no: u32,
        chain: bool,
    },
}

#[derive(Debug, Deserialize)]
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
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
        let PlPgSqlRaiseOption::Inner { expression, .. } = self;
        expression.extract_objects(buffer)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub enum PlPgSqlRaiseOptionType {
    #[serde(rename = "PLPGSQL_RAISEOPTION_ERRCODE")]
    ErrorCode,
    #[serde(rename = "PLPGSQL_RAISEOPTION_MESSAGE")]
    Message,
    #[serde(rename = "PLPGSQL_RAISEOPTION_DETAIL")]
    Detail,
    #[serde(rename = "PLPGSQL_RAISEOPTION_HINT")]
    Hint,
    #[serde(rename = "PLPGSQL_RAISEOPTION_COLUMN")]
    Column,
    #[serde(rename = "PLPGSQL_RAISEOPTION_COLUMN")]
    Constraint,
    #[serde(rename = "PLPGSQL_RAISEOPTION_DATATYPE")]
    DataType,
    #[serde(rename = "PLPGSQL_RAISEOPTION_TABLE")]
    Table,
    #[serde(rename = "PLPGSQL_RAISEOPTION_SCHEMA")]
    Schema,
}

impl ObjectNode for PlPgSqlStatement {
    fn extract_objects(&self, buffer: &mut Vec<SchemaQualifiedName>) -> Result<(), PgDiffError> {
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
                loop_variable,
                body,
                query,
                params,
                ..
            } => {
                loop_variable.extract_objects(buffer)?;
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
                expression, target, ..
            } => {
                expression.extract_objects(buffer)?;
                target.extract_objects(buffer)?;
            }
            PlPgSqlStatement::Commit { .. } => {}
            PlPgSqlStatement::Rollback { .. } => {}
        }
        Ok(())
    }
}

// #[derive(Debug, Deserialize)]
// pub struct PlPgSqlStatement(PlPgSqlStatementType);

#[derive(Debug, Deserialize)]
pub enum PlPgSqlFunction {
    #[serde(rename = "PLpgSQL_function")]
    Inner {
        #[serde(rename = "new_varno", default)]
        new_varialbe_no: u32,
        #[serde(rename = "old_varno", default)]
        old_variable_no: u32,
        #[serde(default)]
        datums: Vec<PlPgSqlVariable>,
        action: PlPgSqlStatement,
    },
}

impl PlPgSqlFunction {
    pub fn get_types(&self) -> Vec<&str> {
        let PlPgSqlFunction::Inner { datums, .. } = self;
        datums
            .iter()
            .filter_map(|v| {
                if let PlPgSqlVariable::Var { data_type, .. } = v {
                    match data_type {
                        PlPgSqlType::Inner { type_name } => Some(type_name.as_str()),
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_objects(&self) -> Result<Vec<SchemaQualifiedName>, PgDiffError> {
        let PlPgSqlFunction::Inner { action, .. } = self;
        let mut result = Vec::new();
        action.extract_objects(&mut result)?;
        Ok(result)
    }
}
