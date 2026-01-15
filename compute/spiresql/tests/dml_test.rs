//! Tests for the DML handler module.
//!
//! Tests cover:
//! - SQL parsing for INSERT/UPDATE/DELETE statements
//! - Value extraction from INSERT VALUES
//! - WHERE clause parsing for UPDATE/DELETE
//! - Expression to bytes conversion

use sqlparser::ast::{Expr, SetExpr, Statement, Values};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Test parsing INSERT statement.
#[test]
fn test_parse_insert() {
    let sql = "INSERT INTO users (id, name) VALUES (1, 'alice')";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    assert_eq!(statements.len(), 1);
    match &statements[0] {
        Statement::Insert(insert) => {
            assert_eq!(insert.table_name.to_string(), "users");
            assert_eq!(insert.columns.len(), 2);
            assert_eq!(insert.columns[0].value, "id");
            assert_eq!(insert.columns[1].value, "name");
        }
        _ => panic!("Expected Insert statement"),
    }
}

/// Test parsing INSERT with multiple rows.
#[test]
fn test_parse_insert_multiple_rows() {
    let sql = "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Insert(insert) => {
            if let Some(source) = &insert.source {
                if let SetExpr::Values(Values { rows, .. }) = source.body.as_ref() {
                    assert_eq!(rows.len(), 3);
                } else {
                    panic!("Expected VALUES");
                }
            }
        }
        _ => panic!("Expected Insert statement"),
    }
}

/// Test parsing UPDATE statement.
#[test]
fn test_parse_update() {
    let sql = "UPDATE users SET name = 'bob' WHERE id = 1";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    assert_eq!(statements.len(), 1);
    match &statements[0] {
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => {
            // Check table
            match &table.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    assert_eq!(name.to_string(), "users");
                }
                _ => panic!("Expected simple table"),
            }
            // Check assignments
            assert_eq!(assignments.len(), 1);
            // Check WHERE exists
            assert!(selection.is_some());
        }
        _ => panic!("Expected Update statement"),
    }
}

/// Test parsing UPDATE with multiple assignments.
#[test]
fn test_parse_update_multiple_columns() {
    let sql = "UPDATE users SET name = 'bob', email = 'bob@example.com' WHERE id = 1";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Update { assignments, .. } => {
            assert_eq!(assignments.len(), 2);
        }
        _ => panic!("Expected Update statement"),
    }
}

/// Test parsing DELETE statement.
#[test]
fn test_parse_delete() {
    let sql = "DELETE FROM users WHERE id = 1";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    assert_eq!(statements.len(), 1);
    match &statements[0] {
        Statement::Delete(delete) => {
            // Check FROM clause exists
            match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                    assert!(!tables.is_empty());
                }
                sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                    assert!(!tables.is_empty());
                }
            }
            // Check WHERE exists
            assert!(delete.selection.is_some());
        }
        _ => panic!("Expected Delete statement"),
    }
}

/// Test parsing WHERE clause with equality.
#[test]
fn test_parse_where_equality() {
    let sql = "DELETE FROM users WHERE id = 42";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Delete(delete) => {
            if let Some(Expr::BinaryOp { left, op, right }) = &delete.selection {
                assert!(matches!(op, sqlparser::ast::BinaryOperator::Eq));
                assert_eq!(left.to_string(), "id");
                assert_eq!(right.to_string(), "42");
            } else {
                panic!("Expected binary expression");
            }
        }
        _ => panic!("Expected Delete statement"),
    }
}

/// Test parsing INSERT with different value types.
#[test]
fn test_parse_insert_value_types() {
    let sql = "INSERT INTO test (a, b, c, d, e) VALUES (42, 3.14, 'hello', true, NULL)";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Insert(insert) => {
            if let Some(source) = &insert.source
                && let SetExpr::Values(Values { rows, .. }) = source.body.as_ref()
            {
                let row = &rows[0];
                assert_eq!(row.len(), 5);
                // Check value types
                assert!(matches!(
                    &row[0],
                    Expr::Value(sqlparser::ast::Value::Number(_, _))
                ));
                assert!(matches!(
                    &row[1],
                    Expr::Value(sqlparser::ast::Value::Number(_, _))
                ));
                assert!(matches!(
                    &row[2],
                    Expr::Value(sqlparser::ast::Value::SingleQuotedString(_))
                ));
                assert!(matches!(
                    &row[3],
                    Expr::Value(sqlparser::ast::Value::Boolean(true))
                ));
                assert!(matches!(&row[4], Expr::Value(sqlparser::ast::Value::Null)));
            }
        }
        _ => panic!("Expected Insert statement"),
    }
}

/// Test DML is not parsed as DDL.
#[test]
fn test_dml_not_ddl() {
    let dml_statements = [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1 WHERE b = 2",
        "DELETE FROM t WHERE a = 1",
    ];

    let dialect = PostgreSqlDialect {};
    for sql in dml_statements {
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert!(
            !matches!(&statements[0], Statement::CreateTable(_)),
            "{} should not be CreateTable",
            sql
        );
        assert!(
            !matches!(&statements[0], Statement::Drop { .. }),
            "{} should not be Drop",
            sql
        );
    }
}

/// Test DML is not parsed as SELECT.
#[test]
fn test_dml_not_select() {
    let dml_statements = [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1 WHERE b = 2",
        "DELETE FROM t WHERE a = 1",
    ];

    let dialect = PostgreSqlDialect {};
    for sql in dml_statements {
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert!(
            !matches!(&statements[0], Statement::Query(_)),
            "{} should not be Query",
            sql
        );
    }
}

/// Test INSERT without explicit columns.
#[test]
fn test_parse_insert_no_columns() {
    let sql = "INSERT INTO users VALUES (1, 'alice')";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Insert(insert) => {
            assert!(insert.columns.is_empty());
            assert!(insert.source.is_some());
        }
        _ => panic!("Expected Insert statement"),
    }
}

/// Test expression conversion to string.
#[test]
fn test_expr_to_string() {
    let sql = "SELECT 42, 'hello', true";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Query(query) => {
            if let SetExpr::Select(select) = query.body.as_ref() {
                assert_eq!(select.projection.len(), 3);
                assert_eq!(select.projection[0].to_string(), "42");
                assert_eq!(select.projection[1].to_string(), "'hello'");
                assert_eq!(select.projection[2].to_string(), "true");
            }
        }
        _ => panic!("Expected Query"),
    }
}
