//! DML statement handler for SpireSQL.
//!
//! Routes INSERT/UPDATE/DELETE statements to SpireDB's DataAccess service via gRPC.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use spire_proto::spiredb::data::{
    TableDeleteRequest, TableInsertRequest, TableUpdateRequest,
    data_access_client::DataAccessClient,
};
use sqlparser::ast::{Expr, ObjectName, SetExpr, Statement, Values};
use std::collections::HashMap;
use tonic::transport::Channel;

/// A row of column-value pairs for INSERT operations.
type InsertRow = Vec<(String, Vec<u8>)>;

/// Handler for DML statements (INSERT/UPDATE/DELETE).
pub struct DmlHandler {
    data_client: DataAccessClient<Channel>,
}

impl DmlHandler {
    /// Create a new DML handler with the given data access client.
    pub fn new(data_client: DataAccessClient<Channel>) -> Self {
        Self { data_client }
    }

    /// Try to execute a DML statement. Returns None if statement is not DML.
    pub async fn try_execute(&mut self, stmt: &Statement) -> PgWireResult<Option<Vec<Response>>> {
        match stmt {
            Statement::Insert(insert) => self
                .insert(&insert.table_name, &insert.columns, &insert.source)
                .await
                .map(Some),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                // Extract table name from TableWithJoins -> TableFactor
                let table_name = extract_table_name_from_factor(&table.relation)?;
                self.update(&table_name, assignments, selection.as_ref())
                    .await
                    .map(Some)
            }
            Statement::Delete(delete) => {
                // Extract table from FromTable
                let table_name = extract_table_name_from_delete(&delete.from)?;
                self.delete(&table_name, delete.selection.as_ref())
                    .await
                    .map(Some)
            }
            _ => Ok(None),
        }
    }

    async fn insert(
        &mut self,
        table: &ObjectName,
        columns: &[sqlparser::ast::Ident],
        source: &Option<Box<sqlparser::ast::Query>>,
    ) -> PgWireResult<Vec<Response>> {
        let table_name = table.to_string();

        // Extract values from source query
        let rows = match source {
            Some(query) => extract_values_from_query(query, columns)?,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "INSERT requires VALUES clause".to_string(),
                ))));
            }
        };

        // Encode rows as simple binary format for now
        // Format: [pk_len:4][pk][value_len:4][value]...
        let arrow_batch = encode_insert_rows(&rows);

        let request = TableInsertRequest {
            table_name: table_name.clone(),
            arrow_batch,
        };

        match self.data_client.table_insert(request).await {
            Ok(response) => {
                let rows_affected = response.into_inner().rows_affected;
                log::info!("Inserted {} rows into '{}'", rows_affected, table_name);
                Ok(vec![Response::Execution(
                    Tag::new("INSERT").with_rows(rows_affected as usize),
                )])
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(),
                format!("Failed to insert: {}", e.message()),
            )))),
        }
    }

    async fn update(
        &mut self,
        table: &ObjectName,
        assignments: &[sqlparser::ast::Assignment],
        selection: Option<&Expr>,
    ) -> PgWireResult<Vec<Response>> {
        let table_name = table.to_string();

        // Extract primary key from WHERE clause (simplified: expects WHERE pk = value)
        let primary_key = match selection {
            Some(expr) => extract_pk_from_where(expr)?,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "UPDATE requires WHERE clause with primary key".to_string(),
                ))));
            }
        };

        // Convert assignments to map
        let updates: HashMap<String, Vec<u8>> = assignments
            .iter()
            .map(|a| {
                let col = assignment_target_to_string(&a.target);
                let val = expr_to_bytes(&a.value);
                (col, val)
            })
            .collect();

        // Encode updates as simple binary (for future use)
        let _arrow_batch = encode_update_values(&updates);

        let request = TableUpdateRequest {
            table_name: table_name.clone(),
            primary_key: primary_key.clone(),
            updates,
        };

        match self.data_client.table_update(request).await {
            Ok(response) => {
                let updated = response.into_inner().updated;
                log::info!("Updated row in '{}'", table_name);
                Ok(vec![Response::Execution(
                    Tag::new("UPDATE").with_rows(if updated { 1 } else { 0 }),
                )])
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(),
                format!("Failed to update: {}", e.message()),
            )))),
        }
    }

    async fn delete(
        &mut self,
        table: &ObjectName,
        selection: Option<&Expr>,
    ) -> PgWireResult<Vec<Response>> {
        let table_name = table.to_string();

        // Extract primary key from WHERE clause
        let primary_key = match selection {
            Some(expr) => extract_pk_from_where(expr)?,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "DELETE requires WHERE clause with primary key".to_string(),
                ))));
            }
        };

        let request = TableDeleteRequest {
            table_name: table_name.clone(),
            primary_key: primary_key.clone(),
        };

        match self.data_client.table_delete(request).await {
            Ok(response) => {
                let deleted = response.into_inner().deleted;
                log::info!("Deleted row from '{}'", table_name);
                Ok(vec![Response::Execution(
                    Tag::new("DELETE").with_rows(if deleted { 1 } else { 0 }),
                )])
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(),
                format!("Failed to delete: {}", e.message()),
            )))),
        }
    }
}

/// Extract values from INSERT ... VALUES query.
fn extract_values_from_query(
    query: &sqlparser::ast::Query,
    columns: &[sqlparser::ast::Ident],
) -> PgWireResult<Vec<InsertRow>> {
    if let SetExpr::Values(Values { rows, .. }) = query.body.as_ref() {
        let col_names: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();

        let result = rows
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .map(|(i, expr)| {
                        let col_name = col_names
                            .get(i)
                            .cloned()
                            .unwrap_or_else(|| format!("col{}", i));
                        (col_name, expr_to_bytes(expr))
                    })
                    .collect()
            })
            .collect();

        Ok(result)
    } else {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "42601".to_string(),
            "Only VALUES clause is supported for INSERT".to_string(),
        ))))
    }
}

/// Convert expression to bytes for storage.
fn expr_to_bytes(expr: &Expr) -> Vec<u8> {
    match expr {
        Expr::Value(v) => match v {
            sqlparser::ast::Value::Number(n, _) => n.as_bytes().to_vec(),
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => s.as_bytes().to_vec(),
            sqlparser::ast::Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
            sqlparser::ast::Value::Null => vec![],
            _ => expr.to_string().into_bytes(),
        },
        _ => expr.to_string().into_bytes(),
    }
}

/// Encode insert rows as simple binary format.
fn encode_insert_rows(rows: &[Vec<(String, Vec<u8>)>]) -> Vec<u8> {
    let mut buf = Vec::new();
    for row in rows {
        // Use first column as PK (simplified)
        if let Some((_, pk_val)) = row.first() {
            // pk_len + pk + value_len + value (all columns as term-encoded map)
            let pk_len = pk_val.len() as u32;
            buf.extend_from_slice(&pk_len.to_be_bytes());
            buf.extend_from_slice(pk_val);

            // Encode remaining columns as simple key=value pairs
            let value = serde_json::to_vec(
                &row.iter()
                    .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
                    .collect::<HashMap<_, _>>(),
            )
            .unwrap_or_default();
            let val_len = value.len() as u32;
            buf.extend_from_slice(&val_len.to_be_bytes());
            buf.extend_from_slice(&value);
        }
    }
    buf
}

/// Encode update values for the TableUpdateRequest (used for arrow_batch field if needed).
fn encode_update_values(_updates: &HashMap<String, Vec<u8>>) -> Vec<u8> {
    // TableUpdateRequest uses the `updates` map field directly
    // This is for the arrow_batch field which stores the full row value
    vec![]
}

/// Extract primary key value from WHERE clause.
/// Simplified: expects `WHERE pk_column = value`.
fn extract_pk_from_where(expr: &Expr) -> PgWireResult<Vec<u8>> {
    match expr {
        Expr::BinaryOp {
            left: _,
            op: sqlparser::ast::BinaryOperator::Eq,
            right,
        } => {
            // Assume right side is the value
            Ok(expr_to_bytes(right))
        }
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "42601".to_string(),
            "WHERE clause must be in format: pk_column = value".to_string(),
        )))),
    }
}

/// Extract table name from TableFactor (used in UPDATE).
fn extract_table_name_from_factor(
    factor: &sqlparser::ast::TableFactor,
) -> PgWireResult<ObjectName> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name.clone()),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "42601".to_string(),
            "Expected simple table name".to_string(),
        )))),
    }
}

/// Extract table name from FromTable (used in DELETE).
fn extract_table_name_from_delete(from: &sqlparser::ast::FromTable) -> PgWireResult<ObjectName> {
    match from {
        sqlparser::ast::FromTable::WithFromKeyword(tables) => {
            if let Some(first) = tables.first() {
                extract_table_name_from_factor(&first.relation)
            } else {
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "DELETE requires FROM clause".to_string(),
                ))))
            }
        }
        sqlparser::ast::FromTable::WithoutKeyword(tables) => {
            if let Some(first) = tables.first() {
                extract_table_name_from_factor(&first.relation)
            } else {
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "42601".to_string(),
                    "DELETE requires table name".to_string(),
                ))))
            }
        }
    }
}

/// Convert AssignmentTarget to column name string.
fn assignment_target_to_string(target: &sqlparser::ast::AssignmentTarget) -> String {
    match target {
        sqlparser::ast::AssignmentTarget::ColumnName(names) => {
            // ObjectName is a newtype around Vec<Ident>, access inner with .0
            names
                .0
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".")
        }
        sqlparser::ast::AssignmentTarget::Tuple(exprs) => exprs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", "),
    }
}
