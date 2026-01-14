//! Predicate-Based Region Pruning
//!
//! Analyzes WHERE clause predicates to extract key bounds for region filtering.
//! Enables skipping shards that can't contain matching data.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::prelude::Expr;

/// Extracted key bounds from predicates.
#[derive(Debug, Clone, Default)]
pub struct KeyBounds {
    /// Lower bound key (inclusive).
    pub start_key: Option<Vec<u8>>,
    /// Upper bound key (exclusive).
    pub end_key: Option<Vec<u8>>,
}

#[allow(dead_code)]
impl KeyBounds {
    /// Create empty bounds (no filtering).
    pub fn unbounded() -> Self {
        Self {
            start_key: None,
            end_key: None,
        }
    }

    /// Check if bounds constrain the key range.
    pub fn is_bounded(&self) -> bool {
        self.start_key.is_some() || self.end_key.is_some()
    }

    /// Get start key slice (empty if unbounded).
    pub fn start_key_slice(&self) -> &[u8] {
        self.start_key.as_deref().unwrap_or(&[])
    }

    /// Get end key slice (empty if unbounded).
    pub fn end_key_slice(&self) -> &[u8] {
        self.end_key.as_deref().unwrap_or(&[])
    }
}

/// Analyze predicates to extract key bounds for region pruning.
///
/// Looks for primary key column comparisons like:
/// - `pk = value` -> point lookup
/// - `pk >= value` or `pk > value` -> lower bound
/// - `pk <= value` or `pk < value` -> upper bound
/// - `pk BETWEEN a AND b` -> range
///
/// # Arguments
/// * `filters` - DataFusion filter expressions
/// * `pk_column` - Name of the primary key column
///
/// # Returns
/// Key bounds for region pruning
pub fn extract_key_bounds(filters: &[Expr], pk_column: &str) -> KeyBounds {
    let mut bounds = KeyBounds::unbounded();

    for filter in filters {
        extract_from_expr(filter, pk_column, &mut bounds);
    }

    if bounds.is_bounded() {
        log::debug!(
            "Extracted key bounds for '{}': start={:?}, end={:?}",
            pk_column,
            bounds.start_key.as_ref().map(|k| k.len()),
            bounds.end_key.as_ref().map(|k| k.len())
        );
    }

    bounds
}

/// Recursively extract bounds from an expression.
fn extract_from_expr(expr: &Expr, pk_column: &str, bounds: &mut KeyBounds) {
    match expr {
        // Binary expressions: pk = value, pk > value, etc.
        Expr::BinaryExpr(binary) => {
            // Check if left side references the PK column
            if let Expr::Column(col) = binary.left.as_ref() {
                if col.name() == pk_column {
                    // Right side should be a literal
                    if let Some(value) = extract_scalar_bytes(binary.right.as_ref()) {
                        match binary.op {
                            Operator::Eq => {
                                // Point lookup: both bounds are the same
                                bounds.start_key = Some(value.clone());
                                // For exact match, end_key is start_key + 1 (conceptually)
                                let mut end = value;
                                increment_key(&mut end);
                                bounds.end_key = Some(end);
                            }
                            Operator::Gt => {
                                // pk > value: start is value + 1
                                let mut start = value;
                                increment_key(&mut start);
                                update_start_key(bounds, start);
                            }
                            Operator::GtEq => {
                                // pk >= value: start is value
                                update_start_key(bounds, value);
                            }
                            Operator::Lt => {
                                // pk < value: end is value
                                update_end_key(bounds, value);
                            }
                            Operator::LtEq => {
                                // pk <= value: end is value + 1
                                let mut end = value;
                                increment_key(&mut end);
                                update_end_key(bounds, end);
                            }
                            _ => {}
                        }
                    }
                }
            }

            // Also check for AND (intersection of bounds)
            if binary.op == Operator::And {
                extract_from_expr(binary.left.as_ref(), pk_column, bounds);
                extract_from_expr(binary.right.as_ref(), pk_column, bounds);
            }
        }

        // Between: pk BETWEEN a AND b
        Expr::Between(between) => {
            if let Expr::Column(col) = between.expr.as_ref() {
                if col.name() == pk_column {
                    if let (Some(low), Some(high)) = (
                        extract_scalar_bytes(between.low.as_ref()),
                        extract_scalar_bytes(between.high.as_ref()),
                    ) {
                        update_start_key(bounds, low);
                        let mut end = high;
                        increment_key(&mut end);
                        update_end_key(bounds, end);
                    }
                }
            }
        }

        _ => {}
    }
}

/// Extract bytes from a scalar literal expression.
fn extract_scalar_bytes(expr: &Expr) -> Option<Vec<u8>> {
    if let Expr::Literal(scalar, _) = expr {
        match scalar {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                Some(s.as_bytes().to_vec())
            }
            ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => Some(b.clone()),
            ScalarValue::Int64(Some(i)) => Some(i.to_be_bytes().to_vec()),
            ScalarValue::Int32(Some(i)) => Some(i.to_be_bytes().to_vec()),
            ScalarValue::UInt64(Some(i)) => Some(i.to_be_bytes().to_vec()),
            ScalarValue::UInt32(Some(i)) => Some(i.to_be_bytes().to_vec()),
            _ => None,
        }
    } else {
        None
    }
}

/// Update start key (take max of existing and new).
fn update_start_key(bounds: &mut KeyBounds, key: Vec<u8>) {
    match &bounds.start_key {
        None => bounds.start_key = Some(key),
        Some(existing) if key > *existing => bounds.start_key = Some(key),
        _ => {}
    }
}

/// Update end key (take min of existing and new).
fn update_end_key(bounds: &mut KeyBounds, key: Vec<u8>) {
    match &bounds.end_key {
        None => bounds.end_key = Some(key),
        Some(existing) if key < *existing => bounds.end_key = Some(key),
        _ => {}
    }
}

/// Increment key by 1 (for exclusive to inclusive conversion).
fn increment_key(key: &mut Vec<u8>) {
    for byte in key.iter_mut().rev() {
        if *byte < 255 {
            *byte += 1;
            return;
        }
        *byte = 0;
    }
    // If all bytes were 255, append a 0
    key.push(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::lit;
    use datafusion::prelude::col;

    #[test]
    fn test_extract_eq_bounds() {
        let filters = vec![col("id").eq(lit("abc"))];
        let bounds = extract_key_bounds(&filters, "id");

        assert!(bounds.is_bounded());
        assert_eq!(bounds.start_key, Some(b"abc".to_vec()));
    }

    #[test]
    fn test_extract_range_bounds() {
        let filters = vec![col("id").gt_eq(lit("a")), col("id").lt(lit("z"))];
        let bounds = extract_key_bounds(&filters, "id");

        assert!(bounds.is_bounded());
        assert_eq!(bounds.start_key, Some(b"a".to_vec()));
        assert_eq!(bounds.end_key, Some(b"z".to_vec()));
    }

    #[test]
    fn test_no_pk_filter() {
        let filters = vec![col("name").eq(lit("test"))];
        let bounds = extract_key_bounds(&filters, "id");

        assert!(!bounds.is_bounded());
    }
}
