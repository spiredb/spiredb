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
