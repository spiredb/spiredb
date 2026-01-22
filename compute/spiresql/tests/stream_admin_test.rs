//! Tests for stream admin module.
//!
//! Covers AdminClient, TopicConfig, TopicInfo, GroupInfo, and related types.

use spiresql::stream::prelude::*;
use std::collections::HashMap;

// ============================================================================
// TopicConfig Tests
// ============================================================================

#[test]
fn test_topic_config_new() {
    let config = TopicConfig::new();

    assert_eq!(config.partitions, 1);
    assert_eq!(config.replication_factor, 1);
}

#[test]
fn test_topic_config_builder_pattern() {
    let config = TopicConfig::new()
        .partitions(8)
        .replication_factor(3)
        .max_entries(1_000_000)
        .retention_ms(86400 * 7 * 1000); // 1 week

    assert_eq!(config.partitions, 8);
    assert_eq!(config.replication_factor, 3);
    assert_eq!(config.max_entries, Some(1_000_000));
    assert_eq!(config.retention_ms, Some(86400 * 7 * 1000));
}

#[test]
fn test_topic_config_clone() {
    let config = TopicConfig::new().partitions(4);
    let cloned = config.clone();

    assert_eq!(config.partitions, cloned.partitions);
}

#[test]
fn test_topic_config_debug() {
    let config = TopicConfig::new();
    let debug = format!("{:?}", config);

    assert!(debug.contains("TopicConfig"));
}

#[test]
fn test_topic_config_default() {
    let config: TopicConfig = Default::default();
    assert_eq!(config.partitions, 0); // Default is 0, new() is 1
}

// ============================================================================
// TopicInfo Tests
// ============================================================================

#[test]
fn test_topic_info_structure() {
    let info = TopicInfo {
        name: "users".to_string(),
        partitions: 4,
        replication_factor: 3,
        config: HashMap::new(),
    };

    assert_eq!(info.name, "users");
    assert_eq!(info.partitions, 4);
}

#[test]
fn test_topic_info_clone() {
    let info = TopicInfo {
        name: "test".to_string(),
        partitions: 1,
        replication_factor: 1,
        config: HashMap::new(),
    };
    let cloned = info.clone();
    assert_eq!(info.name, cloned.name);
}

// ============================================================================
// GroupState Tests
// ============================================================================

#[test]
fn test_group_state_variants() {
    assert_ne!(GroupState::Empty, GroupState::Stable);
    assert_ne!(GroupState::Stable, GroupState::Rebalancing);
    assert_ne!(GroupState::Rebalancing, GroupState::Dead);
}

#[test]
fn test_group_state_clone_copy() {
    let state = GroupState::Stable;
    let cloned = state;
    let copied = state;

    assert_eq!(state, cloned);
    assert_eq!(state, copied);
}

#[test]
fn test_group_state_debug() {
    assert!(format!("{:?}", GroupState::Empty).contains("Empty"));
    assert!(format!("{:?}", GroupState::Stable).contains("Stable"));
    assert!(format!("{:?}", GroupState::Dead).contains("Dead"));
    assert!(format!("{:?}", GroupState::Rebalancing).contains("Rebalancing"));
}

// ============================================================================
// GroupInfo Tests
// ============================================================================

#[test]
fn test_group_info_structure() {
    let info = GroupInfo {
        group_id: "my-group".to_string(),
        state: GroupState::Stable,
        coordinator: Some("node-1".to_string()),
        members: vec![],
    };

    assert_eq!(info.group_id, "my-group");
    assert_eq!(info.state, GroupState::Stable);
    assert!(info.members.is_empty());
}

#[test]
fn test_group_info_clone() {
    let info = GroupInfo {
        group_id: "test".to_string(),
        state: GroupState::Empty,
        coordinator: None,
        members: vec![],
    };
    let cloned = info.clone();
    assert_eq!(info.group_id, cloned.group_id);
}

#[test]
fn test_group_info_no_coordinator() {
    let info = GroupInfo {
        group_id: "orphan".to_string(),
        state: GroupState::Dead,
        coordinator: None,
        members: vec![],
    };

    assert!(info.coordinator.is_none());
}

// ============================================================================
// AdminClient Tests
// ============================================================================

#[test]
fn test_admin_client_new() {
    let admin = AdminClient::new("localhost:6379");
    assert_eq!(admin.bootstrap_servers(), "localhost:6379");
}

#[tokio::test]
async fn test_admin_client_create_topic() {
    let admin = AdminClient::new("localhost:6379");

    let config = TopicConfig::new().partitions(4);
    admin.create_topic("test-topic", config).await.unwrap();
}

#[tokio::test]
async fn test_admin_client_delete_topic() {
    let admin = AdminClient::new("localhost:6379");
    admin.delete_topic("test-topic").await.unwrap();
}

#[tokio::test]
async fn test_admin_client_list_topics() {
    let admin = AdminClient::new("localhost:6379");

    let topics = admin.list_topics().await.unwrap();
    assert!(topics.is_empty()); // Stub returns empty
}

#[tokio::test]
async fn test_admin_client_describe_topic() {
    let admin = AdminClient::new("localhost:6379");

    let info = admin.describe_topic("orders").await.unwrap();
    assert_eq!(info.name, "orders");
}

#[tokio::test]
async fn test_admin_client_delete_consumer_group() {
    let admin = AdminClient::new("localhost:6379");
    admin.delete_consumer_group("group").await.unwrap();
}

#[tokio::test]
async fn test_admin_client_list_consumer_groups() {
    let admin = AdminClient::new("localhost:6379");

    let groups = admin.list_consumer_groups().await.unwrap();
    assert!(groups.is_empty());
}

#[tokio::test]
async fn test_admin_client_describe_consumer_group() {
    let admin = AdminClient::new("localhost:6379");

    let info = admin.describe_consumer_group("my-group").await.unwrap();
    assert_eq!(info.group_id, "my-group");
}

#[tokio::test]
async fn test_admin_client_close() {
    let admin = AdminClient::new("localhost:6379");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn test_admin_client_reset_offsets() {

    let admin = AdminClient::new("localhost:6379");

    // Test all OffsetSpec variants
    admin
        .reset_offsets("group", "topic", OffsetSpec::Earliest)
        .await
        .unwrap();
    admin
        .reset_offsets("group", "topic", OffsetSpec::Latest)
        .await
        .unwrap();
    admin
        .reset_offsets("group", "topic", OffsetSpec::Offset(100))
        .await
        .unwrap();
    admin
        .reset_offsets("group", "topic", OffsetSpec::Timestamp(1700000000000))
        .await
        .unwrap();
}

// ============================================================================
// MemberInfo Tests
// ============================================================================

#[test]
fn test_member_info_structure() {
    

    let member = MemberInfo {
        member_id: "member-1".to_string(),
        client_id: "client-1".to_string(),
        client_host: "192.168.1.1".to_string(),
        assignment: vec![
            TopicPartition::new("topic", 0),
            TopicPartition::new("topic", 1),
        ],
    };

    assert_eq!(member.member_id, "member-1");
    assert_eq!(member.client_id, "client-1");
    assert_eq!(member.assignment.len(), 2);
}

#[test]
fn test_member_info_clone() {

    let member = MemberInfo {
        member_id: "m".to_string(),
        client_id: "c".to_string(),
        client_host: "h".to_string(),
        assignment: vec![],
    };
    let cloned = member.clone();
    assert_eq!(member.member_id, cloned.member_id);
}

// ============================================================================
// OffsetSpec Tests
// ============================================================================

#[test]
fn test_offset_spec_variants() {

    let earliest = OffsetSpec::Earliest;
    let latest = OffsetSpec::Latest;
    let offset = OffsetSpec::Offset(1000);
    let timestamp = OffsetSpec::Timestamp(1700000000000);

    // Test debug
    assert!(format!("{:?}", earliest).contains("Earliest"));
    assert!(format!("{:?}", latest).contains("Latest"));
    assert!(format!("{:?}", offset).contains("1000"));
    assert!(format!("{:?}", timestamp).contains("1700000000000"));
}

#[test]
fn test_offset_spec_clone() {

    let spec = OffsetSpec::Offset(500);
    let cloned = spec.clone();
    assert!(matches!(cloned, OffsetSpec::Offset(500)));
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_topic_config_zero_partitions() {
    // Zero partitions should be allowed at config level
    // (validation would happen on the server)
    let config = TopicConfig::new().partitions(0);
    assert_eq!(config.partitions, 0);
}

#[test]
fn test_admin_client_multiple_servers() {
    let admin = AdminClient::new("node1:6379,node2:6379,node3:6379");
    assert!(admin.bootstrap_servers().contains("node1"));
}
