use super::{parse_plpgsql_function, PlPgSqlElsIf, PlPgSqlExpr, PlPgSqlFunction, PlPgSqlStatement};

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
        label: None,
        body,
        condition: PlPgSqlExpr::Inner { query: condition, .. },
        ..
    }, PlPgSqlStatement::Return { .. }] = &body[..]
    else {
        panic!("Actions within block is not 1 while statement\n{body:#?}");
    };
    
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

/*
FOR I
FOR S
FOR C
FOREACH
RETURN
RETURN NEXT
RETURN QUERY
RAISE
ASSERT
SQL
EXECUTE ''
FOR EXECUTE
GET DIAGNOSTICS
OPEN
FETCH
MOVE
CLOSE
PERFORM
CALL
COMMIT
ROLLBACK
 */
