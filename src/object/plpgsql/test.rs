use super::{
    parse_plpgsql_function, CursorOption, FetchDirection, PlPgSqlDiagnosticsItem,
    PlPgSqlDiagnoticsKind, PlPgSqlElsIf, PlPgSqlExpr, PlPgSqlFunction, PlPgSqlOpenCursor,
    PlPgSqlRaiseLogLevel, PlPgSqlRaiseOption, PlPgSqlRaiseOptionType, PlPgSqlStatement,
    PlPgSqlVariable, RawParseMode, RowField,
};
use lazy_regex::{lazy_regex, Lazy, Regex};

static CLEAN_QUERY: Lazy<Regex> = lazy_regex!("\\s+");

#[test]
fn parse_plpgsql_function_should_parse_assign_statement() {
    let function_block = include_str!("./../../../test-files/plpgsql-assign-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::Assign {
        expression: PlPgSqlExpr::Inner { query, .. },
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not an if statement\n{body:#?}");
    };
    assert_eq!("test_int := test_int + 1", query);
}

#[test]
fn parse_plpgsql_function_should_parse_if_statement() {
    let function_block = include_str!("./../../../test-files/plpgsql-if-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block")
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::If {
        condition: PlPgSqlExpr::Inner { query, .. },
        then_body,
        elsif_body,
        else_body: Some(else_body),
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not an if statement\n{body:#?}");
    };

    assert_eq!("test_int = 10", query);
    assert!(matches!(&then_body[..], [PlPgSqlStatement::Assign { .. }]));
    let Some(elsif_body) = elsif_body else {
        panic!("ELSIF body was empty. {body:?}");
    };
    assert_eq!(1, elsif_body.len());
    let PlPgSqlElsIf::Inner {
        condition: PlPgSqlExpr::Inner { query, .. },
        statements,
        ..
    } = elsif_body.first().unwrap();
    assert_eq!("test_int = 1", query);
    assert!(matches!(&statements[..], [PlPgSqlStatement::Assign { .. }]));
    assert!(matches!(&else_body[..], [PlPgSqlStatement::Assign { .. }]));
}

#[test]
fn parse_plpgsql_function_should_parse_loop_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-loop-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::Loop {
        label: Some(label),
        body: body1,
        ..
    }, PlPgSqlStatement::Loop {
        label: None,
        body: body2,
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not 2 loop statements\n{body:#?}");
    };
    assert_eq!("ablock", label);
    validate_loop(Some(label), body1);
    validate_loop(None, body2);
}

fn validate_loop(label: Option<&String>, body: &[PlPgSqlStatement]) {
    let [PlPgSqlStatement::Assign { .. }, PlPgSqlStatement::ExitOrContinue {
        is_exit: false,
        label: label1,
        condition: Some(PlPgSqlExpr::Inner {
            query: condition1, ..
        }),
        ..
    }, PlPgSqlStatement::ExitOrContinue {
        is_exit: true,
        label: label2,
        condition: Some(PlPgSqlExpr::Inner {
            query: condition2, ..
        }),
        ..
    }, PlPgSqlStatement::ExitOrContinue {
        is_exit: false,
        label: label3,
        condition: None,
        ..
    }] = body
    else {
        panic!("Loop is not the expected statements\n{body:#?}");
    };
    assert_eq!(label, label1.as_ref());
    assert_eq!(label, label2.as_ref());
    assert_eq!(label, label3.as_ref());
    assert_eq!("test_int < 5", condition1);
    assert_eq!("test_int >= 10", condition2);
}

#[test]
fn parse_plpgsql_function_should_parse_while_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-while-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::While {
        label,
        body,
        condition: PlPgSqlExpr::Inner {
            query: condition, ..
        },
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block is not 1 while statement\n{body:#?}");
    };

    assert!(label.is_none());
    assert_eq!("test_int <= 15", condition);
    assert!(matches!(&body[..], [PlPgSqlStatement::Assign { .. }]));
}

#[test]
fn parse_plpgsql_function_should_parse_fori_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-fori-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::ForI {
        label: None,
        lower: PlPgSqlExpr::Inner { query: lower1, .. },
        upper: PlPgSqlExpr::Inner { query: upper1, .. },
        step: None,
        reverse: false,
        body: body1,
        ..
    }, PlPgSqlStatement::ForI {
        label: None,
        lower: PlPgSqlExpr::Inner { query: lower2, .. },
        upper: PlPgSqlExpr::Inner { query: upper2, .. },
        step: Some(PlPgSqlExpr::Inner { query: step, .. }),
        reverse: false,
        body: body2,
        ..
    }, PlPgSqlStatement::ForI {
        label: None,
        lower: PlPgSqlExpr::Inner { query: lower3, .. },
        upper: PlPgSqlExpr::Inner { query: upper3, .. },
        step: None,
        reverse: true,
        body: body3,
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not 3 fori statements\n{body:#?}");
    };

    assert_eq!("1", lower1);
    assert_eq!("1", lower2);
    assert_eq!("10", lower3);
    assert_eq!("10", upper1);
    assert_eq!("10", upper2);
    assert_eq!("1", upper3);
    assert_eq!("2", step);
    assert!(matches!(&body1[..], [PlPgSqlStatement::Assign { .. }]));
    assert!(matches!(&body2[..], [PlPgSqlStatement::Assign { .. }]));
    assert!(matches!(&body3[..], [PlPgSqlStatement::Assign { .. }]));
}

#[test]
fn parse_plpgsql_function_should_parse_fors_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-fors-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::ForS {
        label,
        body,
        query: PlPgSqlExpr::Inner { query, .. },
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block is not fors statement\n{body:#?}");
    };

    assert!(label.is_none());
    assert_eq!("(select 1 as \"id\")", query);
    assert!(matches!(&body[..], [PlPgSqlStatement::Assign { .. }]));
}

#[test]
fn parse_plpgsql_function_should_parse_dyn_fors_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-dyn-fors-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::DynForS {
        label,
        body,
        query: PlPgSqlExpr::Inner { query, .. },
        params: Some(params),
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block is not fors statement\n{body:#?}");
    };

    assert!(label.is_none());
    assert_eq!("'select $1 as \"id\"'", query);
    assert!(matches!(&body[..], [PlPgSqlStatement::Assign { .. }]));
    assert!(matches!(&params[..], [PlPgSqlExpr::Inner { .. }]));
}

// This test cannot currently work due to an issue with the pg_query library
// #[test]
#[allow(unused)]
fn parse_plpgsql_function_should_parse_forc_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-forc-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::ForC {
        label: None,
        body: body1,
        query_args: Some(query_args1),
        ..
    }, PlPgSqlStatement::ForC {
        label: None,
        body: body2,
        query_args: Some(query_args2),
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not 2 forc statements\n{body:#?}");
    };

    let [PlPgSqlExpr::Inner { query, .. }] = &query_args1[..] else {
        panic!("Expected 1 cursor query arg\n{:#?}", query_args1);
    };
    assert_eq!("id:=1", query);
    assert!(query_args2.is_empty());
    assert!(matches!(&body1[..], [PlPgSqlStatement::Assign { .. }]));
    assert!(matches!(&body2[..], [PlPgSqlStatement::Assign { .. }]));
}

#[test]
fn parse_plpgsql_function_should_parse_foreach_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-foreach-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::Foreach {
        slice_dimension,
        expression: PlPgSqlExpr::Inner { query, .. },
        body,
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block is not foreach statement\n{body:#?}");
    };

    assert_eq!(1, *slice_dimension);
    assert_eq!("array[array[1,2,3,4],array[5,6,7,8]]", query);

    let [PlPgSqlStatement::Foreach {
        slice_dimension,
        expression: PlPgSqlExpr::Inner { query, .. },
        body,
        ..
    }] = &body[..]
    else {
        panic!("Actions within block is not foreach statement\n{body:#?}");
    };

    assert_eq!(0, *slice_dimension);
    assert_eq!("arr", query);
    assert!(matches!(&body[..], [PlPgSqlStatement::Assign { .. }]));
}

#[test]
fn parse_plpgsql_function_should_parse_return_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-return-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::Return {
        expression: Some(PlPgSqlExpr::Inner { query, .. }),
        ..
    }] = &body[..]
    else {
        panic!("Actions within block is not return statement\n{body:#?}");
    };

    assert_eq!("1", query);
}

#[test]
fn parse_plpgsql_function_should_parse_return_next_query_statement() {
    let function_block =
        include_str!("./../../../test-files/pl-pgsql-return-next-query-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::ReturnNext {
        expression: PlPgSqlExpr::Inner {
            query: next_value, ..
        },
        ..
    }, PlPgSqlStatement::ReturnQuery {
        query: Some(PlPgSqlExpr::Inner {
            query: static_query,
            ..
        }),
        dynamic_query: None,
        params: None,
        ..
    }, PlPgSqlStatement::ReturnQuery {
        query: None,
        dynamic_query: Some(PlPgSqlExpr::Inner {
            query: dyn_query, ..
        }),
        params: Some(params),
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not return statements\n{body:#?}");
    };

    assert_eq!("1", next_value);
    assert_eq!("select 1", static_query);
    assert_eq!("'select $1'", dyn_query);
    let [PlPgSqlExpr::Inner { query, .. }] = &params[..] else {
        panic!("Param are not a single expression\n{params:#?}");
    };
    assert_eq!("1", query);
}

#[test]
fn parse_plpgsql_function_should_parse_raise_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-raise-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [raise1, raise2, raise3, raise4, raise5, raise6, PlPgSqlStatement::Return { .. }] =
        &body[..]
    else {
        panic!("Actions within block are not raise statements\n{body:#?}");
    };

    verify_raise_statement(
        raise1,
        PlPgSqlRaiseLogLevel::Error,
        None,
        None,
        None,
        Some(vec![
            create_raise_option(PlPgSqlRaiseOptionType::Message, "'Message'"),
            create_raise_option(PlPgSqlRaiseOptionType::Detail, "'Detail'"),
            create_raise_option(PlPgSqlRaiseOptionType::Hint, "'Hint'"),
            create_raise_option(PlPgSqlRaiseOptionType::ErrorCode, "'unique_violation'"),
            create_raise_option(PlPgSqlRaiseOptionType::Column, "'column'"),
            create_raise_option(PlPgSqlRaiseOptionType::Constraint, "'constraint'"),
            create_raise_option(PlPgSqlRaiseOptionType::DataType, "'datatype'"),
            create_raise_option(PlPgSqlRaiseOptionType::Table, "'table'"),
            create_raise_option(PlPgSqlRaiseOptionType::Schema, "'schema'"),
        ]),
    );
    verify_raise_statement(
        raise2,
        PlPgSqlRaiseLogLevel::Warning,
        None,
        Some("This will not happen %s"),
        Some(vec![create_expression("'test'")]),
        Some(vec![create_raise_option(
            PlPgSqlRaiseOptionType::Detail,
            "'Detail'",
        )]),
    );
    verify_raise_statement(
        raise3,
        PlPgSqlRaiseLogLevel::Notice,
        None,
        Some("This will not happen %s"),
        Some(vec![create_expression("'test'")]),
        None,
    );
    verify_raise_statement(
        raise4,
        PlPgSqlRaiseLogLevel::Info,
        Some("division_by_zero"),
        None,
        None,
        None,
    );
    verify_raise_statement(
        raise5,
        PlPgSqlRaiseLogLevel::Log,
        Some("division_by_zero"),
        None,
        None,
        None,
    );
    verify_raise_statement(
        raise6,
        PlPgSqlRaiseLogLevel::Debug,
        Some("division_by_zero"),
        None,
        None,
        None,
    );
}

fn create_expression(expression: &str) -> PlPgSqlExpr {
    PlPgSqlExpr::Inner {
        query: expression.into(),
        parse_mode: RawParseMode::default(),
    }
}

fn create_raise_option(
    option_type: PlPgSqlRaiseOptionType,
    expression: &str,
) -> PlPgSqlRaiseOption {
    PlPgSqlRaiseOption::Inner {
        option_type,
        expression: create_expression(expression),
    }
}

#[allow(clippy::too_many_arguments)]
fn verify_raise_statement(
    raise_statement: &PlPgSqlStatement,
    level: PlPgSqlRaiseLogLevel,
    condition: Option<&str>,
    raise_message: Option<&str>,
    raise_params: Option<Vec<PlPgSqlExpr>>,
    raise_options: Option<Vec<PlPgSqlRaiseOption>>,
) {
    let PlPgSqlStatement::Raise {
        error_log_level,
        condition_name,
        message,
        params,
        options,
        ..
    } = raise_statement
    else {
        panic!("Statement is not a raise\n{raise_statement:#?}");
    };

    assert_eq!(&level, error_log_level);
    assert_eq!(condition, condition_name.as_deref());
    assert_eq!(raise_message, message.as_deref());
    assert_eq!(&raise_params, params);
    assert_eq!(&raise_options, options);
}

#[test]
fn parse_plpgsql_function_should_parse_assert_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-assert-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::Assert {
        condition: PlPgSqlExpr::Inner {
            query: condition1, ..
        },
        message: Some(PlPgSqlExpr::Inner { query: message, .. }),
        ..
    }, PlPgSqlStatement::Assert {
        condition: PlPgSqlExpr::Inner {
            query: condition2, ..
        },
        message: None,
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not assert statements\n{body:#?}");
    };

    assert_eq!("'Assertion failed'", message);
    assert_eq!("test_int != 0", condition1);
    assert_eq!("test_int != 0", condition2);
}

#[test]
fn parse_plpgsql_function_should_parse_sql_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-sql-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::ExecSql {
        sql_statement: PlPgSqlExpr::Inner { query: query1, .. },
        is_dml_statement: false,
        into: true,
        is_strict: true,
        target: Some(PlPgSqlVariable::Row {
            fields: fields1, ..
        }),
        ..
    }, PlPgSqlStatement::ExecSql {
        sql_statement: PlPgSqlExpr::Inner { query: query2, .. },
        is_dml_statement: false,
        into: true,
        is_strict: false,
        target: Some(PlPgSqlVariable::Row {
            fields: fields2, ..
        }),
        ..
    }, PlPgSqlStatement::ExecSql {
        sql_statement: PlPgSqlExpr::Inner { query: query3, .. },
        is_dml_statement: false,
        into: false,
        is_strict: false,
        target: None,
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not SQL statements\n{body:#?}");
    };

    assert_eq!(
        "select oid from pg_catalog.pg_type offset 0 fetch first row only",
        CLEAN_QUERY.replace_all(query1, " ")
    );
    assert_eq!(
        "select oid, typname from pg_catalog.pg_type offset 0 fetch first row only",
        CLEAN_QUERY.replace_all(query2, " ")
    );
    assert!(matches!(&fields1[..], [RowField { .. }]));
    assert!(matches!(&fields2[..], [RowField { .. }, RowField { .. }]));
    assert_eq!(
        "update test_table set test_field = 'test' where id = 1",
        CLEAN_QUERY.replace_all(query3, " ")
    );
}

#[test]
fn parse_plpgsql_function_should_parse_dyn_sql_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-dyn-sql-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::DynExecute {
        query: PlPgSqlExpr::Inner { query: query1, .. },
        into: true,
        is_strict: true,
        target: Some(PlPgSqlVariable::Row {
            fields: fields1, ..
        }),
        params: None,
        ..
    }, PlPgSqlStatement::DynExecute {
        query: PlPgSqlExpr::Inner { query: query2, .. },
        into: true,
        is_strict: false,
        target: Some(PlPgSqlVariable::Row {
            fields: fields2, ..
        }),
        params: None,
        ..
    }, PlPgSqlStatement::DynExecute {
        query: PlPgSqlExpr::Inner { query: query3, .. },
        into: false,
        is_strict: false,
        target: None,
        params: Some(params),
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not SQL statements\n{body:#?}");
    };

    assert_eq!(
        "'select oid from pg_catalog.pg_type offset 0 fetch first row only'",
        CLEAN_QUERY.replace_all(query1, " ")
    );
    assert_eq!(
        "'select oid, typname from pg_catalog.pg_type offset 0 fetch first row only'",
        CLEAN_QUERY.replace_all(query2, " ")
    );
    assert!(matches!(&fields1[..], [RowField { .. }]));
    assert!(matches!(&fields2[..], [RowField { .. }, RowField { .. }]));
    assert_eq!(
        "'update test_table set test_field = $1 where id = $2'",
        CLEAN_QUERY.replace_all(query3, " ")
    );
    let [PlPgSqlExpr::Inner { query: param1, .. }, PlPgSqlExpr::Inner { query: param2, .. }] =
        &params[..]
    else {
        panic!("Actions within block are not SQL statements\n{body:#?}");
    };
    assert_eq!("'test'", param1);
    assert_eq!("1", param2);
}

#[test]
fn parse_plpgsql_function_should_parse_get_diagnostics_statement() {
    let function_block =
        include_str!("./../../../test-files/pl-pgsql-get-diagnostics-statement.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::GetDiagnostics {
        is_stacked: true,
        diagnostics_items,
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block is not a get diagnostics statement\n{body:#?}");
    };

    assert!(matches!(
        &diagnostics_items[..],
        [
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::ReturnedSqlState,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::ColumnName,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::ConstraintName,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::DataTypeName,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::MessageText,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::TableName,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::SchemaName,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::ErrorDetails,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::ErrorHint,
                ..
            },
            PlPgSqlDiagnosticsItem::Inner {
                kind: PlPgSqlDiagnoticsKind::ErrorContext,
                ..
            }
        ]
    ))
}

// This test cannot currently work due to an issue with the pg_query library
// #[test]
#[allow(unused)]
fn parse_plpgsql_function_should_parse_cursor_statement() {
    let function_block = include_str!("./../../../test-files/pl-pgsql-cursor-statements.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        datums,
        ..
    } = function.first().unwrap();

    assert_eq!(4, datums.len(), "Datums: {datums:#?}");
    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::Open {
        cursor_options: CursorOption::NoScroll,
        query:
            PlPgSqlOpenCursor::Query {
                query: PlPgSqlExpr::Inner { query: query1, .. },
            },
        ..
    }, PlPgSqlStatement::Close { .. }, PlPgSqlStatement::Open {
        cursor_options: CursorOption::Scroll,
        query:
            PlPgSqlOpenCursor::Execute {
                dyn_query: PlPgSqlExpr::Inner { query: query2, .. },
                params: Some(params),
            },
        ..
    }, PlPgSqlStatement::Fetch {
        target: Some(PlPgSqlVariable::Row { .. }),
        direction: FetchDirection::Forward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: false,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: None,
        direction: FetchDirection::Backward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: true,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: Some(PlPgSqlVariable::Row { .. }),
        direction: FetchDirection::Forward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: false,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: None,
        direction: FetchDirection::Backward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: true,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: Some(PlPgSqlVariable::Row { .. }),
        direction: FetchDirection::Forward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: false,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: None,
        direction: FetchDirection::Backward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: true,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: Some(PlPgSqlVariable::Row { .. }),
        direction: FetchDirection::Absolute,
        fetch_count: Some(1),
        fetch_count_expr: None,
        is_move: false,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: None,
        direction: FetchDirection::Backward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: true,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: Some(PlPgSqlVariable::Row { .. }),
        direction: FetchDirection::Relative,
        fetch_count: Some(1),
        fetch_count_expr: None,
        is_move: false,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Fetch {
        target: None,
        direction: FetchDirection::Backward,
        fetch_count: None,
        fetch_count_expr: None,
        is_move: true,
        returns_multiple_rows: false,
        ..
    }, PlPgSqlStatement::Close { .. }, PlPgSqlStatement::Open {
        cursor_options: CursorOption::None,
        query:
            PlPgSqlOpenCursor::Args {
                query_args:
                    PlPgSqlExpr::Inner {
                        query: query_args, ..
                    },
            },
        ..
    }, PlPgSqlStatement::Close { .. }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not cursor statements\n{body:#?}");
    };

    assert_eq!("select 1 as \"id\"", query1);
    assert_eq!("'select $1 as \"id\"'", query2);
    let [PlPgSqlExpr::Inner { query: param, .. }] = &params[..] else {
        panic!("Params is not a single parameter\n{params:#?}");
    };
    assert_eq!("1", param);
    assert_eq!("(1)", query_args);
}

#[test]
fn parse_plpgsql_function_should_parse_perform_call_statements() {
    let function_block =
        include_str!("./../../../test-files/pl-pgsql-perform-call-statements.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);
    let [PlPgSqlStatement::Perform {
        expression: PlPgSqlExpr::Inner { query: perform, .. },
        ..
    }, PlPgSqlStatement::Call {
        expression: PlPgSqlExpr::Inner { query: call1, .. },
        is_call: true,
        ..
    }, PlPgSqlStatement::Call {
        expression: PlPgSqlExpr::Inner { query: do_expr, .. },
        is_call: false,
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block are not perform and call statements\n{body:#?}");
    };

    assert_eq!("SELECT pg_notify('','')", perform);
    assert_eq!("call test_procedure()", call1);
    assert_eq!("do $do$update test set fld = true;$do$", do_expr);
}

#[test]
fn parse_plpgsql_function_should_parse_commit_rollback_statements() {
    let function_block =
        include_str!("./../../../test-files/pl-pgsql-commit-rollback-statements.pgsql");
    let function = parse_plpgsql_function(function_block).unwrap();
    let PlPgSqlFunction::Inner {
        new_variable_no,
        old_variable_no,
        action,
        ..
    } = function.first().unwrap();

    assert_eq!(0, *new_variable_no);
    assert_eq!(0, *old_variable_no);
    let PlPgSqlStatement::Block {
        label,
        body,
        declare_var_count,
        ..
    } = action
    else {
        panic!("Top level function block is not a block");
    };
    assert!(label.is_none());
    assert_eq!(0, *declare_var_count);

    assert!(matches!(
        &body[..],
        [
            PlPgSqlStatement::Commit { chain: true, .. },
            PlPgSqlStatement::Rollback { chain: true, .. },
            PlPgSqlStatement::Return { .. }
        ]
    ));
}
