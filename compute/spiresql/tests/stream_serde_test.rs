//! Tests for stream serde module.
//!
//! Covers Serializer, Deserializer, Serde traits and implementations.

use serde::{Deserialize, Serialize};
use spiresql::stream::prelude::*;

// ============================================================================
// JsonSerde Tests
// ============================================================================

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestUser {
    id: u64,
    name: String,
    active: bool,
}

#[test]
fn test_json_serde_serialize() {
    let serde = JsonSerde;
    let user = TestUser {
        id: 1,
        name: "Alice".to_string(),
        active: true,
    };

    let bytes = serde.serialize(&user).unwrap();
    assert!(!bytes.is_empty());

    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["id"], 1);
    assert_eq!(json["name"], "Alice");
}

#[test]
fn test_json_serde_deserialize() {
    let serde = JsonSerde;
    let json = r#"{"id":42,"name":"Bob","active":false}"#;

    let user: TestUser = serde.deserialize(json.as_bytes()).unwrap();
    assert_eq!(user.id, 42);
    assert_eq!(user.name, "Bob");
    assert!(!user.active);
}

#[test]
fn test_json_serde_roundtrip() {
    let serde = JsonSerde;
    let original = TestUser {
        id: 100,
        name: "Charlie".to_string(),
        active: true,
    };

    let bytes = serde.serialize(&original).unwrap();
    let restored: TestUser = serde.deserialize(&bytes).unwrap();

    assert_eq!(original, restored);
}

#[test]
fn test_json_serde_complex_types() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: String,
        items: Vec<String>,
        total: f64,
        metadata: std::collections::HashMap<String, String>,
    }

    let serde = JsonSerde;
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("source".to_string(), "web".to_string());

    let order = Order {
        id: "ord-123".to_string(),
        items: vec!["item1".to_string(), "item2".to_string()],
        total: 99.99,
        metadata,
    };

    let bytes = serde.serialize(&order).unwrap();
    let restored: Order = serde.deserialize(&bytes).unwrap();

    assert_eq!(order, restored);
}

#[test]
fn test_json_serde_invalid_json() {
    let serde = JsonSerde;
    let invalid = b"not valid json";

    let result: Result<TestUser, _> = serde.deserialize(invalid);
    assert!(result.is_err());
}

#[test]
fn test_json_serde_empty() {
    let serde = JsonSerde;
    let empty = b"";

    let result: Result<TestUser, _> = serde.deserialize(empty);
    assert!(result.is_err());
}

#[test]
fn test_json_serde_null() {
    let serde = JsonSerde;
    let null = b"null";

    let result: Result<Option<TestUser>, _> = serde.deserialize(null);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// ============================================================================
// BytesSerde Tests
// ============================================================================

#[test]
fn test_bytes_serde_serialize() {
    let serde = BytesSerde;
    let data = vec![1u8, 2, 3, 4, 5];

    let bytes = serde.serialize(&data).unwrap();
    assert_eq!(bytes, data);
}

#[test]
fn test_bytes_serde_deserialize() {
    let serde = BytesSerde;
    let data = vec![10u8, 20, 30];

    let result: Vec<u8> = serde.deserialize(&data).unwrap();
    assert_eq!(result, data);
}

#[test]
fn test_bytes_serde_roundtrip() {
    let serde = BytesSerde;
    let original: Vec<u8> = (0..=255).collect();

    let bytes = serde.serialize(&original).unwrap();
    let restored: Vec<u8> = serde.deserialize(&bytes).unwrap();

    assert_eq!(original, restored);
}

#[test]
fn test_bytes_serde_empty() {
    let serde = BytesSerde;
    let empty: Vec<u8> = vec![];

    let bytes = serde.serialize(&empty).unwrap();
    let restored: Vec<u8> = serde.deserialize(&bytes).unwrap();

    assert!(restored.is_empty());
}

#[test]
fn test_bytes_serde_large() {
    let serde = BytesSerde;
    let large: Vec<u8> = vec![0xAB; 1024 * 1024]; // 1MB

    let bytes = serde.serialize(&large).unwrap();
    let restored: Vec<u8> = serde.deserialize(&bytes).unwrap();

    assert_eq!(large.len(), restored.len());
}

// ============================================================================
// StringSerde Tests
// ============================================================================

#[test]
fn test_string_serde_serialize() {
    let serde = StringSerde;
    let data = "Hello, World!".to_string();

    let bytes = serde.serialize(&data).unwrap();
    assert_eq!(bytes, b"Hello, World!");
}

#[test]
fn test_string_serde_deserialize() {
    let serde = StringSerde;
    let data = b"Hello, Rust!";

    let result: String = serde.deserialize(data).unwrap();
    assert_eq!(result, "Hello, Rust!");
}

#[test]
fn test_string_serde_roundtrip() {
    let serde = StringSerde;
    let original = "Test string with special chars: 日本語 🎉".to_string();

    let bytes = serde.serialize(&original).unwrap();
    let restored: String = serde.deserialize(&bytes).unwrap();

    assert_eq!(original, restored);
}

#[test]
fn test_string_serde_empty() {
    let serde = StringSerde;
    let empty = String::new();

    let bytes = serde.serialize(&empty).unwrap();
    let restored: String = serde.deserialize(&bytes).unwrap();

    assert!(restored.is_empty());
}

#[test]
fn test_string_serde_invalid_utf8() {
    let serde = StringSerde;
    let invalid = vec![0xFF, 0xFE]; // Invalid UTF-8

    let result: Result<String, _> = serde.deserialize(&invalid);
    assert!(result.is_err());
}

#[test]
fn test_string_serde_unicode() {
    let serde = StringSerde;
    let unicode = "日本語テスト 中文测试 한국어테스트 Привет мир".to_string();

    let bytes = serde.serialize(&unicode).unwrap();
    let restored: String = serde.deserialize(&bytes).unwrap();

    assert_eq!(unicode, restored);
}

// ============================================================================
// Serde Trait Tests
// ============================================================================

fn round_trip_test<S, T>(serde: S, value: T)
where
    S: Serde<T>,
    T: PartialEq + std::fmt::Debug,
{
    let bytes = serde.serialize(&value).unwrap();
    let restored = serde.deserialize(&bytes).unwrap();
    assert_eq!(value, restored);
}

#[test]
fn test_serde_trait_with_json() {
    let serde = JsonSerde;
    let user = TestUser {
        id: 1,
        name: "Test".to_string(),
        active: true,
    };
    round_trip_test(serde, user);
}

#[test]
fn test_serde_trait_with_bytes() {
    let serde = BytesSerde;
    let data = vec![1u8, 2, 3, 4, 5];
    round_trip_test(serde, data);
}

#[test]
fn test_serde_trait_with_string() {
    let serde = StringSerde;
    let data = "test string".to_string();
    round_trip_test(serde, data);
}

// ============================================================================
// Serializer Trait Tests
// ============================================================================

fn test_serializer<S: Serializer<TestUser>>(serializer: &S) {
    let user = TestUser {
        id: 1,
        name: "Test".to_string(),
        active: true,
    };
    let result = serializer.serialize(&user);
    assert!(result.is_ok());
    assert!(!result.unwrap().is_empty());
}

#[test]
fn test_serializer_trait() {
    test_serializer(&JsonSerde);
}

// ============================================================================
// Deserializer Trait Tests
// ============================================================================

fn test_deserializer<D: Deserializer<TestUser>>(deserializer: &D, data: &[u8]) {
    let result = deserializer.deserialize(data);
    assert!(result.is_ok());
}

#[test]
fn test_deserializer_trait() {
    let data = r#"{"id":1,"name":"Test","active":true}"#.as_bytes();
    test_deserializer(&JsonSerde, data);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_json_serde_nested_structures() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Inner {
        value: i32,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Outer {
        inner: Inner,
        list: Vec<Inner>,
    }

    let serde = JsonSerde;
    let data = Outer {
        inner: Inner { value: 1 },
        list: vec![Inner { value: 2 }, Inner { value: 3 }],
    };

    let bytes = serde.serialize(&data).unwrap();
    let restored: Outer = serde.deserialize(&bytes).unwrap();

    assert_eq!(data, restored);
}

#[test]
fn test_json_serde_optional_fields() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct WithOptional {
        required: String,
        optional: Option<String>,
    }

    let serde = JsonSerde;

    // With optional present
    let with = WithOptional {
        required: "req".to_string(),
        optional: Some("opt".to_string()),
    };
    let bytes = serde.serialize(&with).unwrap();
    let restored: WithOptional = serde.deserialize(&bytes).unwrap();
    assert_eq!(with, restored);

    // Without optional
    let without = WithOptional {
        required: "req".to_string(),
        optional: None,
    };
    let bytes = serde.serialize(&without).unwrap();
    let restored: WithOptional = serde.deserialize(&bytes).unwrap();
    assert_eq!(without, restored);
}

#[test]
fn test_string_serde_multiline() {
    let serde = StringSerde;
    let multiline = "Line 1\nLine 2\nLine 3".to_string();

    let bytes = serde.serialize(&multiline).unwrap();
    let restored: String = serde.deserialize(&bytes).unwrap();

    assert_eq!(multiline, restored);
}
