//! DML statement handler for SpireSQL.
//!
//! Routes INSERT/UPDATE/DELETE statements to SpireDB's DataAccess service via gRPC.

use crate::pool::ConnectionPool;
use crate::routing::RegionRouter;
use crate::topology::ClusterTopology;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use spire_proto::spiredb::cluster::GetTableIdRequest;
use spire_proto::spiredb::cluster::schema_service_client::SchemaServiceClient;
use spire_proto::spiredb::data::{TableDeleteRequest, TableInsertRequest, TableUpdateRequest};
use sqlparser::ast::{Expr, ObjectName, SetExpr, Statement, Values};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

/// A row of column-value pairs for INSERT operations.
type InsertRow = Vec<(String, Vec<u8>)>;

/// Handler for DML statements (INSERT/UPDATE/DELETE).
pub struct DmlHandler {
    region_router: Arc<RegionRouter>,
    connection_pool: Arc<ConnectionPool>,
    cluster_topology: Arc<ClusterTopology>,
    schema_client: SchemaServiceClient<Channel>,
}

impl DmlHandler {
    /// Create a new DML handler with router, pool, and schema client.
    pub fn new(
        region_router: Arc<RegionRouter>,
        connection_pool: Arc<ConnectionPool>,
        cluster_topology: Arc<ClusterTopology>,
        schema_client: SchemaServiceClient<Channel>,
    ) -> Self {
        Self {
            region_router,
            connection_pool,
            cluster_topology,
            schema_client,
        }
    }

    /// Get table ID from SpireDB SchemaService (uses :erlang.phash2 on server).
    async fn get_table_id(&self, table_name: &str) -> PgWireResult<u64> {
        let request = GetTableIdRequest {
            table_name: table_name.to_string(),
        };
        let mut client = self.schema_client.clone();
        let response = client
            .get_table_id(request)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        Ok(response.into_inner().table_id)
    }

    /// Find the region that contains the given key using key-range matching.
    fn find_region_for_key<'a>(
        regions: &'a [crate::routing::RegionInfo],
        key: &[u8],
    ) -> Option<&'a crate::routing::RegionInfo> {
        regions.iter().find(|r| {
            // Region contains key if: start_key <= key < end_key
            // Empty start_key means -infinity, empty end_key means +infinity
            let after_start = r.start_key.is_empty() || key >= r.start_key.as_slice();
            let before_end = r.end_key.is_empty() || key < r.end_key.as_slice();
            after_start && before_end
        })
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
                // table is &TableWithJoins from match
                let table_name = extract_table_name_from_joins(table)?;
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

        // Get table_id from SpireDB (uses :erlang.phash2 on server)
        let table_id = self.get_table_id(&table_name).await?;

        // Get all regions for this table
        let regions = self
            .region_router
            .get_table_regions(&table_name)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        if regions.is_empty() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(),
                format!("No regions found for table '{}'", table_name),
            ))));
        }

        // Group rows by region using key-range matching
        let mut batch_by_region: HashMap<u64, Vec<InsertRow>> = HashMap::new();

        for row in rows {
            let row_ref: &InsertRow = &row;
            if let Some((_col, pk_val)) = row_ref.first() {
                // Encode the full key (table_id prefix + pk)
                let key = encode_table_key(table_id, pk_val);

                // Find region using key-range matching
                if let Some(region) = Self::find_region_for_key(&regions, &key) {
                    batch_by_region
                        .entry(region.region_id)
                        .or_default()
                        .push(row);
                } else {
                    log::warn!(
                        "No region found for key (table_id={}, pk_len={})",
                        table_id,
                        pk_val.len()
                    );
                }
            }
        }

        let mut total_rows_affected = 0;
        let mut errors = Vec::new();

        // Route batches to correct region leaders
        for (region_id, region_rows) in batch_by_region {
            let region_info = regions.iter().find(|r| r.region_id == region_id);

            if let Some(info) = region_info {
                let leader_id = info.leader_store_id;
                if let Some(addr) = self.cluster_topology.get_store_address(leader_id) {
                    match self.connection_pool.get_data_access_client(&addr).await {
                        Ok(mut client) => {
                            let arrow_batch = encode_insert_rows(&region_rows);
                            let request = TableInsertRequest {
                                table_name: table_name.clone(),
                                arrow_batch,
                            };

                            match client.table_insert(request).await {
                                Ok(resp) => total_rows_affected += resp.into_inner().rows_affected,
                                Err(e) => {
                                    errors.push(format!("Region {}: {}", region_id, e));
                                }
                            }
                        }
                        Err(e) => {
                            errors.push(format!(
                                "Connect to leader {} at {}: {}",
                                leader_id, addr, e
                            ));
                        }
                    }
                } else {
                    errors.push(format!("No address for leader store {}", leader_id));
                }
            }
        }

        if !errors.is_empty() {
            log::error!("INSERT partial failures: {:?}", errors);
        }

        log::info!(
            "Inserted {} rows into '{}'",
            total_rows_affected,
            table_name
        );
        Ok(vec![Response::Execution(Tag::new(&format!(
            "INSERT 0 {}",
            total_rows_affected
        )))])
    }

    async fn update(
        &mut self,
        table_name: &str,
        assignments: &[sqlparser::ast::Assignment],
        selection: Option<&Expr>,
    ) -> PgWireResult<Vec<Response>> {
        // Extract primary key from WHERE clause
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

        // Get table_id from SpireDB (uses :erlang.phash2 on server)
        let table_id = self.get_table_id(table_name).await?;
        let key = encode_table_key(table_id, &primary_key);

        // Get regions and find correct one using key-range matching
        let regions = self
            .region_router
            .get_table_regions(table_name)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let region_info = Self::find_region_for_key(&regions, &key);

        if let Some(info) = region_info {
            let leader_id = info.leader_store_id;
            if let Some(addr) = self.cluster_topology.get_store_address(leader_id) {
                match self.connection_pool.get_data_access_client(&addr).await {
                    Ok(mut client) => {
                        let request = TableUpdateRequest {
                            table_name: table_name.to_string(),
                            primary_key: primary_key.clone(),
                            updates,
                        };
                        match client.table_update(request).await {
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
                    Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "58000".to_string(),
                        format!("Failed to connect to leader: {}", e),
                    )))),
                }
            } else {
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "58000".to_string(),
                    format!("Address not found for leader store {}", leader_id),
                ))))
            }
        } else {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(),
                format!("No region found for key in table {}", table_name),
            ))))
        }
    }

    async fn delete(
        &mut self,
        table_name: &str,
        selection: Option<&Expr>,
    ) -> PgWireResult<Vec<Response>> {
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

        // Get table_id from SpireDB (uses :erlang.phash2 on server)
        let table_id = self.get_table_id(table_name).await?;
        let key = encode_table_key(table_id, &primary_key);

        // Get regions and find correct one using key-range matching
        let regions = self
            .region_router
            .get_table_regions(table_name)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let region_info = Self::find_region_for_key(&regions, &key);

        if let Some(info) = region_info {
            let leader_id = info.leader_store_id;
            if let Some(addr) = self.cluster_topology.get_store_address(leader_id) {
                match self.connection_pool.get_data_access_client(&addr).await {
                    Ok(mut client) => {
                        let request = TableDeleteRequest {
                            table_name: table_name.to_string(),
                            primary_key: primary_key.clone(),
                        };

                        match client.table_delete(request).await {
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
                    Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "58000".to_string(),
                        format!("Failed to connect to leader: {}", e),
                    )))),
                }
            } else {
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "58000".to_string(),
                    format!("Address not found for leader store {}", leader_id),
                ))))
            }
        } else {
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P01".to_string(),
                format!("No region found for key in table {}", table_name),
            ))))
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
fn encode_insert_rows(rows: &[InsertRow]) -> Vec<u8> {
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

/// Extract form FromTable -> String
fn extract_table_name_from_delete(from: &sqlparser::ast::FromTable) -> PgWireResult<String> {
    match from {
        sqlparser::ast::FromTable::WithFromKeyword(tables) => {
            if let Some(first) = tables.first() {
                extract_table_name_from_joins(first)
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
                extract_table_name_from_joins(first)
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

/// Extract table name from TableWithJoins.
fn extract_table_name_from_joins(
    table_with_joins: &sqlparser::ast::TableWithJoins,
) -> PgWireResult<String> {
    extract_table_name_from_factor(&table_with_joins.relation)
}

/// Extract table name from TableFactor.
fn extract_table_name_from_factor(factor: &sqlparser::ast::TableFactor) -> PgWireResult<String> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "42601".to_string(),
            "Expected simple table name".to_string(),
        )))),
    }
}

fn assignment_target_to_string(target: &sqlparser::ast::AssignmentTarget) -> String {
    match target {
        sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
        _ => "unknown".to_string(),
    }
}

fn encode_table_key(table_id: u64, pk: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + pk.len());
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(pk);
    key
}
