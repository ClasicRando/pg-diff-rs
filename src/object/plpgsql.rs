use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub enum PlPgSqlStatementType {
    Block,
    Assign,
    If,
    Case,
    Loop,
    While,
    ForI,
    ForS,
    ForC,
    Foreach,
    Exit,
    Return,
    ReturnNext,
    ReturnQuery,
    Raise,
    Assert,
    ExecSql,
    DynExecute
}

#[derive(Debug, Deserialize)]
pub struct PlPgSqlStatement {
    command_type: PlPgSqlStatementType,
    line_no: i32,
}
