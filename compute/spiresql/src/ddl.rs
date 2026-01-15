//! DDL statement handler for SpireSQL.
//!
//! Routes CREATE/DROP TABLE/INDEX statements to SpireDB's SchemaService via gRPC.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use spire_proto::spiredb::cluster::{
    ColumnDef, ColumnType, CreateIndexRequest, CreateTableRequest, DropIndexRequest,
    DropTableRequest, schema_service_client::SchemaServiceClient,
};
use sqlparser::ast::{
    ColumnDef as SqlColumnDef, ColumnOption, DataType as SqlDataType, ObjectName, Statement,
};
use tonic::transport::Channel;

/// Handler for DDL statements (CREATE/DROP TABLE/INDEX).
pub struct DdlHandler {
    schema_client: SchemaServiceClient<Channel>,
}

impl DdlHandler {
    /// Create a new DDL handler with the given schema service client.
    pub fn new(schema_client: SchemaServiceClient<Channel>) -> Self {
        Self { schema_client }
    }

    /// Try to execute a DDL statement. Returns None if statement is not DDL.
    pub async fn try_execute(&mut self, stmt: &Statement) -> PgWireResult<Option<Vec<Response>>> {
        match stmt {
            Statement::CreateTable(create) => self
                .create_table(&create.name, &create.columns, &create.constraints)
                .await
                .map(Some),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => match object_type {
                sqlparser::ast::ObjectType::Table => {
                    self.drop_table(&names[0], *if_exists).await.map(Some)
                }
                sqlparser::ast::ObjectType::Index => {
                    self.drop_index(&names[0], *if_exists).await.map(Some)
                }
                _ => Ok(None),
            },
            Statement::CreateIndex(create_index) => self
                .create_index(
                    create_index.name.as_ref(),
                    &create_index.table_name,
                    &create_index.columns,
                )
                .await
                .map(Some),
            _ => Ok(None),
        }
    }

    async fn create_table(
        &mut self,
        name: &ObjectName,
        columns: &[SqlColumnDef],
        constraints: &[sqlparser::ast::TableConstraint],
    ) -> PgWireResult<Vec<Response>> {
        let table_name = name.to_string();

        // Convert columns to proto format
        let proto_columns: Vec<ColumnDef> = columns
            .iter()
            .map(|col| {
                let (col_type, precision, scale, vector_dim) = sql_type_to_proto(&col.data_type);
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ColumnOption::NotNull));

                ColumnDef {
                    name: col.name.value.clone(),
                    r#type: col_type.into(),
                    nullable,
                    default_value: vec![],
                    precision,
                    scale,
                    vector_dim,
                    list_elem: ColumnType::TypeInt8.into(),
                }
            })
            .collect();

        // Extract primary key from constraints
        let primary_key = extract_primary_key(constraints);

        let request = CreateTableRequest {
            name: table_name.clone(),
            columns: proto_columns,
            primary_key,
        };

        match self.schema_client.create_table(request).await {
            Ok(response) => {
                let table_id = response.into_inner().table_id;
                log::info!("Created table '{}' with id {}", table_name, table_id);
                Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))])
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P07".to_string(), // duplicate_table
                format!("Failed to create table: {}", e.message()),
            )))),
        }
    }

    async fn drop_table(
        &mut self,
        name: &ObjectName,
        if_exists: bool,
    ) -> PgWireResult<Vec<Response>> {
        let table_name = name.to_string();

        let request = DropTableRequest {
            name: table_name.clone(),
        };

        match self.schema_client.drop_table(request).await {
            Ok(_) => {
                log::info!("Dropped table '{}'", table_name);
                Ok(vec![Response::Execution(Tag::new("DROP TABLE"))])
            }
            Err(e) => {
                if if_exists && e.code() == tonic::Code::NotFound {
                    Ok(vec![Response::Execution(Tag::new("DROP TABLE"))])
                } else {
                    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "42P01".to_string(), // undefined_table
                        format!("Failed to drop table: {}", e.message()),
                    ))))
                }
            }
        }
    }

    async fn create_index(
        &mut self,
        index_name: Option<&ObjectName>,
        table_name: &ObjectName,
        columns: &[sqlparser::ast::OrderByExpr],
    ) -> PgWireResult<Vec<Response>> {
        let idx_name = index_name
            .map(|n| n.to_string())
            .unwrap_or_else(|| format!("{}_idx", table_name));
        let tbl_name = table_name.to_string();
        let col_names: Vec<String> = columns.iter().map(|c| c.expr.to_string()).collect();

        let request = CreateIndexRequest {
            name: idx_name.clone(),
            table_name: tbl_name,
            r#type: 0, // BTREE
            columns: col_names,
            params: std::collections::HashMap::new(),
        };

        match self.schema_client.create_index(request).await {
            Ok(response) => {
                let index_id = response.into_inner().index_id;
                log::info!("Created index '{}' with id {}", idx_name, index_id);
                Ok(vec![Response::Execution(Tag::new("CREATE INDEX"))])
            }
            Err(e) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42P07".to_string(),
                format!("Failed to create index: {}", e.message()),
            )))),
        }
    }

    async fn drop_index(
        &mut self,
        name: &ObjectName,
        if_exists: bool,
    ) -> PgWireResult<Vec<Response>> {
        let index_name = name.to_string();

        let request = DropIndexRequest {
            name: index_name.clone(),
        };

        match self.schema_client.drop_index(request).await {
            Ok(_) => {
                log::info!("Dropped index '{}'", index_name);
                Ok(vec![Response::Execution(Tag::new("DROP INDEX"))])
            }
            Err(e) => {
                if if_exists && e.code() == tonic::Code::NotFound {
                    Ok(vec![Response::Execution(Tag::new("DROP INDEX"))])
                } else {
                    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "42704".to_string(), // undefined_object
                        format!("Failed to drop index: {}", e.message()),
                    ))))
                }
            }
        }
    }
}

/// Convert SQL data type to SpireDB ColumnType.
fn sql_type_to_proto(dt: &SqlDataType) -> (ColumnType, u32, u32, u32) {
    match dt {
        SqlDataType::TinyInt(_) => (ColumnType::TypeInt8, 0, 0, 0),
        SqlDataType::SmallInt(_) => (ColumnType::TypeInt16, 0, 0, 0),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => (ColumnType::TypeInt32, 0, 0, 0),
        SqlDataType::BigInt(_) => (ColumnType::TypeInt64, 0, 0, 0),
        SqlDataType::Real => (ColumnType::TypeFloat32, 0, 0, 0),
        SqlDataType::Float(_) | SqlDataType::Double | SqlDataType::DoublePrecision => {
            (ColumnType::TypeFloat64, 0, 0, 0)
        }
        SqlDataType::Boolean => (ColumnType::TypeBool, 0, 0, 0),
        SqlDataType::Char(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => (ColumnType::TypeString, 0, 0, 0),
        SqlDataType::Binary(_) | SqlDataType::Blob(_) | SqlDataType::Bytea => {
            (ColumnType::TypeBytes, 0, 0, 0)
        }
        SqlDataType::Date => (ColumnType::TypeDate, 0, 0, 0),
        SqlDataType::Timestamp(_, _) | SqlDataType::Datetime(_) => {
            (ColumnType::TypeTimestamp, 0, 0, 0)
        }
        SqlDataType::Decimal(info) | SqlDataType::Numeric(info) => {
            let (p, s) = match info {
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u32, *s as u32),
                sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u32, 0),
                sqlparser::ast::ExactNumberInfo::None => (38, 10),
            };
            (ColumnType::TypeDecimal, p, s, 0)
        }
        SqlDataType::Array(_) => (ColumnType::TypeList, 0, 0, 0),
        // Custom VECTOR type (parsed as custom type)
        SqlDataType::Custom(name, args)
            if name.0.first().map(|i| i.value.to_uppercase()) == Some("VECTOR".to_string()) =>
        {
            let dim = args
                .first()
                .and_then(|a| a.to_string().parse().ok())
                .unwrap_or(128);
            (ColumnType::TypeVector, 0, 0, dim)
        }
        _ => (ColumnType::TypeBytes, 0, 0, 0),
    }
}

/// Extract primary key columns from table constraints.
fn extract_primary_key(constraints: &[sqlparser::ast::TableConstraint]) -> Vec<String> {
    for constraint in constraints {
        if let sqlparser::ast::TableConstraint::PrimaryKey { columns, .. } = constraint {
            return columns.iter().map(|c| c.value.clone()).collect();
        }
    }
    vec![]
}
