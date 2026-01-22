//! Serialization/Deserialization traits for streaming API

use super::error::StreamError;

/// Serializer trait for converting types to bytes
pub trait Serializer<T> {
    /// Serialize a value to bytes
    fn serialize(&self, value: &T) -> Result<Vec<u8>, StreamError>;
}

/// Deserializer trait for converting bytes to types
pub trait Deserializer<T> {
    /// Deserialize bytes to a value
    fn deserialize(&self, data: &[u8]) -> Result<T, StreamError>;
}

/// Combined serializer/deserializer (requires Sized types)
pub trait Serde<T>: Serializer<T> + Deserializer<T> {}

impl<T, S> Serde<T> for S where S: Serializer<T> + Deserializer<T> {}

/// JSON serializer/deserializer using serde_json
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonSerde;

impl<T: serde::Serialize> Serializer<T> for JsonSerde {
    fn serialize(&self, value: &T) -> Result<Vec<u8>, StreamError> {
        serde_json::to_vec(value).map_err(|e| StreamError::Serde(e.to_string()))
    }
}

impl<T: serde::de::DeserializeOwned> Deserializer<T> for JsonSerde {
    fn deserialize(&self, data: &[u8]) -> Result<T, StreamError> {
        serde_json::from_slice(data).map_err(|e| StreamError::Serde(e.to_string()))
    }
}

/// Raw bytes passthrough (no-op serializer)
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSerde;

impl Serializer<Vec<u8>> for BytesSerde {
    fn serialize(&self, value: &Vec<u8>) -> Result<Vec<u8>, StreamError> {
        Ok(value.clone())
    }
}

impl Deserializer<Vec<u8>> for BytesSerde {
    fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, StreamError> {
        Ok(data.to_vec())
    }
}

/// String serializer (UTF-8)
#[derive(Debug, Clone, Copy, Default)]
pub struct StringSerde;

impl Serializer<String> for StringSerde {
    fn serialize(&self, value: &String) -> Result<Vec<u8>, StreamError> {
        Ok(value.as_bytes().to_vec())
    }
}

impl Deserializer<String> for StringSerde {
    fn deserialize(&self, data: &[u8]) -> Result<String, StreamError> {
        String::from_utf8(data.to_vec()).map_err(|e| StreamError::Serde(e.to_string()))
    }
}
