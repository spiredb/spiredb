//! Tests for the topology module.
//!
//! Tests cover:
//! - Store address lookup
//! - Store info management
//! - Store count tracking

use spiresql::topology::StoreInfo;

/// Test StoreInfo structure.
#[test]
fn test_store_info_structure() {
    let store = StoreInfo {
        id: 12345,
        address: "http://spiredb-0:50052".to_string(),
    };

    assert_eq!(store.id, 12345);
    assert_eq!(store.address, "http://spiredb-0:50052");
}

/// Test StoreInfo cloning.
#[test]
fn test_store_info_clone() {
    let store = StoreInfo {
        id: 1,
        address: "http://localhost:50052".to_string(),
    };
    let cloned = store.clone();

    assert_eq!(store.id, cloned.id);
    assert_eq!(store.address, cloned.address);
}

/// Test StoreInfo debug formatting.
#[test]
fn test_store_info_debug() {
    let store = StoreInfo {
        id: 42,
        address: "http://test:50052".to_string(),
    };
    let debug_str = format!("{:?}", store);

    assert!(debug_str.contains("42"));
    assert!(debug_str.contains("test"));
}

/// Test multiple stores.
#[test]
fn test_multiple_stores() {
    let stores = [
        StoreInfo {
            id: 1,
            address: "http://store1:50052".to_string(),
        },
        StoreInfo {
            id: 2,
            address: "http://store2:50052".to_string(),
        },
        StoreInfo {
            id: 3,
            address: "http://store3:50052".to_string(),
        },
    ];

    assert_eq!(stores.len(), 3);
    assert_eq!(stores[0].id, 1);
    assert_eq!(stores[1].id, 2);
    assert_eq!(stores[2].id, 3);
}
