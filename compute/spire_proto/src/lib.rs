//! SpireDB Protocol Buffers
//!
//! This crate contains the compiled Protocol Buffers definitions used for communication
//! between SpireDB components.

pub mod spiredb {
    pub mod cluster {
        tonic::include_proto!("spiredb.cluster");
    }
    pub mod data {
        tonic::include_proto!("spiredb.data");
    }
    pub mod internal {
        tonic::include_proto!("spiredb.internal");
    }
}
