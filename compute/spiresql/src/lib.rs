#![doc(
    html_logo_url = "https://raw.githubusercontent.com/spiredb/spiredb/master/art/spire-square.svg"
)]
//! SpireSQL Library
//!
//! This crate provides the comprehensive interface for SpireDB, enabling SQL querying, real-time
//! streaming, vector search, and more. It is designed to be used both as a standalone server and
//! as a library for programmatic access to SpireDB's unified capabilities.
//!
//! # Modules
//!
//! - [`sql`] - SQL execution infrastructure (context, DDL, DML, distributed execution)
//! - [`stream`] - Kafka-like streaming API (Producer, Consumer, CDC)

pub mod sql;
pub mod stream;
pub mod vector;
