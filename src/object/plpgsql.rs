use serde::Deserialize;

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

#[derive(Debug, Deserialize)]
pub enum PlPgSqlExpr {
    #[serde(rename = "PLpgSQL_expr")]
    Inner {
        #[serde(rename = "parseMode")]
        parse_mode: i32,
        query: String,
    },
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

#[derive(Debug, Deserialize)]
pub struct PlPgSqlCaseWhen {
    #[serde(rename = "lineno")]
    line_no: u32,
    #[serde(rename = "expr")]
    expression: PlPgSqlExpr,
    #[serde(rename = "stmts")]
    statements: Vec<PlPgSqlStatement>,
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
    Inner {},
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
        params: Option<Vec<String>>,
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
