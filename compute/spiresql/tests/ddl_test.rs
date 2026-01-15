//! Tests for the DDL handler module.
//!
//! Tests cover:
//! - SQL parsing for CREATE/DROP TABLE/INDEX statements  
//! - Column type conversion from SQL to proto
//! - Primary key extraction from constraints

use sqlparser::ast::{ColumnOption, DataType as SqlDataType, Statement, TableConstraint};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Test parsing CREATE TABLE statement.
#[test]
fn test_parse_create_table() {
    let sql = "CREATE TABLE users (id INT, name VARCHAR(100), PRIMARY KEY (id))";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    assert_eq!(statements.len(), 1);
    match &statements[0] {
        Statement::CreateTable(create) => {
            assert_eq!(create.name.to_string(), "users");
            assert_eq!(create.columns.len(), 2);
            assert_eq!(create.columns[0].name.value, "id");
            assert_eq!(create.columns[1].name.value, "name");
        }
        _ => panic!("Expected CreateTable statement"),
    }
}

/// Test parsing DROP TABLE statement.
#[test]
fn test_parse_drop_table() {
    let sql = "DROP TABLE users";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    assert_eq!(statements.len(), 1);
    match &statements[0] {
        Statement::Drop {
            object_type, names, ..
        } => {
            assert!(matches!(object_type, sqlparser::ast::ObjectType::Table));
            assert_eq!(names[0].to_string(), "users");
        }
        _ => panic!("Expected Drop statement"),
    }
}

/// Test parsing DROP TABLE IF EXISTS.
#[test]
fn test_parse_drop_table_if_exists() {
    let sql = "DROP TABLE IF EXISTS users";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Drop {
            if_exists, names, ..
        } => {
            assert!(*if_exists);
            assert_eq!(names[0].to_string(), "users");
        }
        _ => panic!("Expected Drop statement"),
    }
}

/// Test parsing CREATE INDEX statement.
#[test]
fn test_parse_create_index() {
    let sql = "CREATE INDEX users_name_idx ON users (name)";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    assert_eq!(statements.len(), 1);
    match &statements[0] {
        Statement::CreateIndex(create_index) => {
            assert_eq!(create_index.table_name.to_string(), "users");
            assert_eq!(create_index.columns.len(), 1);
        }
        _ => panic!("Expected CreateIndex statement"),
    }
}

/// Test parsing DROP INDEX statement.
#[test]
fn test_parse_drop_index() {
    let sql = "DROP INDEX users_name_idx";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::Drop {
            object_type, names, ..
        } => {
            assert!(matches!(object_type, sqlparser::ast::ObjectType::Index));
            assert_eq!(names[0].to_string(), "users_name_idx");
        }
        _ => panic!("Expected Drop statement"),
    }
}

/// Test parsing various column types.
#[test]
fn test_parse_column_types() {
    let sql = r#"
        CREATE TABLE test_types (
            a SMALLINT,
            b INT,
            c BIGINT,
            d FLOAT,
            e DOUBLE PRECISION,
            f BOOLEAN,
            g VARCHAR(255),
            h TEXT,
            i BYTEA,
            j DATE,
            k TIMESTAMP
        )
    "#;
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::CreateTable(create) => {
            assert_eq!(create.columns.len(), 11);
            // Check specific types
            assert!(matches!(
                create.columns[0].data_type,
                SqlDataType::SmallInt(_)
            ));
            assert!(matches!(create.columns[1].data_type, SqlDataType::Int(_)));
            assert!(matches!(
                create.columns[2].data_type,
                SqlDataType::BigInt(_)
            ));
            assert!(matches!(
                create.columns[4].data_type,
                SqlDataType::DoublePrecision
            ));
            assert!(matches!(create.columns[5].data_type, SqlDataType::Boolean));
            assert!(matches!(create.columns[7].data_type, SqlDataType::Text));
            assert!(matches!(create.columns[8].data_type, SqlDataType::Bytea));
            assert!(matches!(create.columns[9].data_type, SqlDataType::Date));
        }
        _ => panic!("Expected CreateTable"),
    }
}

/// Test parsing NOT NULL constraint.
#[test]
fn test_parse_not_null_constraint() {
    let sql = "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100))";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::CreateTable(create) => {
            let id_col = &create.columns[0];
            let has_not_null = id_col
                .options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull));
            assert!(has_not_null, "id column should have NOT NULL");

            let name_col = &create.columns[1];
            let name_has_not_null = name_col
                .options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull));
            assert!(!name_has_not_null, "name column should be nullable");
        }
        _ => panic!("Expected CreateTable"),
    }
}

/// Test parsing primary key constraint.
#[test]
fn test_parse_primary_key_constraint() {
    let sql = "CREATE TABLE users (id INT, name VARCHAR(100), PRIMARY KEY (id))";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::CreateTable(create) => {
            let pk_constraint = create
                .constraints
                .iter()
                .find(|c| matches!(c, TableConstraint::PrimaryKey { .. }));
            assert!(
                pk_constraint.is_some(),
                "Should have PRIMARY KEY constraint"
            );

            if let Some(TableConstraint::PrimaryKey { columns, .. }) = pk_constraint {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].value, "id");
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

/// Test parsing composite primary key.
#[test]
fn test_parse_composite_primary_key() {
    let sql = "CREATE TABLE order_items (order_id INT, product_id INT, qty INT, PRIMARY KEY (order_id, product_id))";
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();

    match &statements[0] {
        Statement::CreateTable(create) => {
            if let Some(TableConstraint::PrimaryKey { columns, .. }) = create
                .constraints
                .iter()
                .find(|c| matches!(c, TableConstraint::PrimaryKey { .. }))
            {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].value, "order_id");
                assert_eq!(columns[1].value, "product_id");
            } else {
                panic!("Should have PRIMARY KEY constraint");
            }
        }
        _ => panic!("Expected CreateTable"),
    }
}

/// Test DDL is not parsed as SELECT.
#[test]
fn test_ddl_not_select() {
    let ddl_statements = [
        "CREATE TABLE t (id INT)",
        "DROP TABLE t",
        "CREATE INDEX idx ON t (col)",
        "DROP INDEX idx",
    ];

    let dialect = PostgreSqlDialect {};
    for sql in ddl_statements {
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        assert!(
            !matches!(&statements[0], Statement::Query(_)),
            "{} should not be parsed as Query",
            sql
        );
    }
}
