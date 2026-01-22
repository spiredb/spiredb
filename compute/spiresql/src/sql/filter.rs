//! Filter serialization for predicate pushdown.
//!
//! Converts DataFusion `Expr` to JSON wire format for SpireDB backend.
//!
//! # Wire Format
//!
//! ```json
//! {
//!   "op": "and",
//!   "args": [
//!     {"op": "eq", "col": "id", "val": {"int": 42}},
//!     {"op": "gt", "col": "age", "val": {"int": 18}}
//!   ]
//! }
//! ```

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use serde_json::{Value, json};

/// Serialize a filter expression to JSON bytes for wire transfer.
pub fn serialize_filter(filters: &[Expr]) -> Vec<u8> {
    if filters.is_empty() {
        return Vec::new();
    }

    // Combine multiple filters with AND
    let json_value = if filters.len() == 1 {
        expr_to_json(&filters[0])
    } else {
        let args: Vec<Value> = filters.iter().map(expr_to_json).collect();
        json!({
            "op": "and",
            "args": args
        })
    };

    match serde_json::to_vec(&json_value) {
        Ok(bytes) => bytes,
        Err(e) => {
            log::warn!("Failed to serialize filter: {}", e);
            Vec::new()
        }
    }
}

/// Convert a single Expr to JSON.
fn expr_to_json(expr: &Expr) -> Value {
    match expr {
        // Binary expressions (comparisons, AND, OR)
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => binary_to_json(left, *op, right),

        // NOT expressions
        Expr::Not(inner) => {
            json!({
                "op": "not",
                "arg": expr_to_json(inner)
            })
        }

        // IS NULL
        Expr::IsNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                json!({
                    "op": "is_null",
                    "col": col.name.clone()
                })
            } else {
                json!({"op": "true"}) // Fallback
            }
        }

        // IS NOT NULL
        Expr::IsNotNull(inner) => {
            if let Expr::Column(col) = inner.as_ref() {
                json!({
                    "op": "not",
                    "arg": {
                        "op": "is_null",
                        "col": col.name.clone()
                    }
                })
            } else {
                json!({"op": "true"})
            }
        }

        // BETWEEN
        Expr::Between(between) => {
            if let Expr::Column(col) = between.expr.as_ref() {
                json!({
                    "op": "between",
                    "col": col.name.clone(),
                    "low": expr_to_json(&between.low),
                    "high": expr_to_json(&between.high)
                })
            } else {
                json!({"op": "true"})
            }
        }

        // IN list
        Expr::InList(in_list) => {
            if let Expr::Column(col) = in_list.expr.as_ref() {
                let vals: Vec<Value> = in_list.list.iter().map(expr_to_json).collect();
                json!({
                    "op": "in",
                    "col": col.name.clone(),
                    "vals": vals
                })
            } else {
                json!({"op": "true"})
            }
        }

        // Literal values (DataFusion 52+: second field is optional metadata)
        Expr::Literal(scalar, _) => scalar_to_json(scalar),

        // Column reference (shouldn't appear at top level, but handle it)
        Expr::Column(col) => {
            json!({"col": col.name.clone()})
        }

        // Unsupported expressions - return true (no filtering)
        _ => {
            log::debug!("Unsupported filter expression: {:?}", expr);
            json!({"op": "true"})
        }
    }
}

/// Convert binary expression to JSON.
fn binary_to_json(left: &Expr, op: Operator, right: &Expr) -> Value {
    let op_str = match op {
        Operator::Eq => "eq",
        Operator::NotEq => "ne",
        Operator::Lt => "lt",
        Operator::LtEq => "le",
        Operator::Gt => "gt",
        Operator::GtEq => "ge",
        Operator::And => "and",
        Operator::Or => "or",
        Operator::LikeMatch => "like",
        _ => {
            log::debug!("Unsupported operator: {:?}", op);
            return json!({"op": "true"});
        }
    };

    // AND/OR have args array
    if op == Operator::And || op == Operator::Or {
        return json!({
            "op": op_str,
            "args": [expr_to_json(left), expr_to_json(right)]
        });
    }

    // Comparison: extract column and value
    match (left, right) {
        (Expr::Column(col), val) => {
            json!({
                "op": op_str,
                "col": col.name.clone(),
                "val": expr_to_json(val)
            })
        }
        (val, Expr::Column(col)) => {
            // Flip comparison for reversed order (e.g., 5 > age -> age < 5)
            let flipped_op = match op_str {
                "lt" => "gt",
                "le" => "ge",
                "gt" => "lt",
                "ge" => "le",
                other => other,
            };
            json!({
                "op": flipped_op,
                "col": col.name.clone(),
                "val": expr_to_json(val)
            })
        }
        _ => {
            // Both sides are complex expressions - not supported
            json!({"op": "true"})
        }
    }
}

/// Convert scalar value to JSON.
fn scalar_to_json(scalar: &ScalarValue) -> Value {
    match scalar {
        ScalarValue::Int8(Some(v)) => json!({"int": *v}),
        ScalarValue::Int16(Some(v)) => json!({"int": *v}),
        ScalarValue::Int32(Some(v)) => json!({"int": *v}),
        ScalarValue::Int64(Some(v)) => json!({"int": *v}),
        ScalarValue::UInt8(Some(v)) => json!({"int": *v}),
        ScalarValue::UInt16(Some(v)) => json!({"int": *v}),
        ScalarValue::UInt32(Some(v)) => json!({"int": *v}),
        ScalarValue::UInt64(Some(v)) => json!({"int": *v}),
        ScalarValue::Float32(Some(v)) => json!({"float": *v}),
        ScalarValue::Float64(Some(v)) => json!({"float": *v}),
        ScalarValue::Utf8(Some(v)) => json!({"str": v}),
        ScalarValue::LargeUtf8(Some(v)) => json!({"str": v}),
        ScalarValue::Boolean(Some(v)) => json!({"bool": *v}),
        ScalarValue::Binary(Some(v)) => {
            use base64::Engine;
            json!({"bytes": base64::engine::general_purpose::STANDARD.encode(v)})
        }
        ScalarValue::LargeBinary(Some(v)) => {
            use base64::Engine;
            json!({"bytes": base64::engine::general_purpose::STANDARD.encode(v)})
        }
        ScalarValue::Null => json!({"null": true}),
        _ => json!({"null": true}), // Unknown/None values treated as null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn test_serialize_eq() {
        let expr = col("id").eq(lit(42i64));
        let json = serialize_filter(&[expr]);
        let parsed: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["op"], "eq");
        assert_eq!(parsed["col"], "id");
        assert_eq!(parsed["val"]["int"], 42);
    }

    #[test]
    fn test_serialize_and() {
        let expr = col("id").eq(lit(42i64)).and(col("age").gt(lit(18i64)));
        let json = serialize_filter(&[expr]);
        let parsed: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed["op"], "and");
    }

    #[test]
    fn test_empty_filters() {
        let json = serialize_filter(&[]);
        assert!(json.is_empty());
    }
}
