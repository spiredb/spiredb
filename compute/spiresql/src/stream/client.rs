//! Stream client trait and RESP client implementation.
//!
//! Defines the interface for communicating with SpireDB stream backend.
#![allow(dead_code)]

use parking_lot::Mutex;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::error::StreamError;
use super::types::{ConsumerRecord, RecordMetadata};

/// Result type for stream operations
pub type StreamResult<T> = Result<T, StreamError>;

/// Trait for stream client implementations
///
/// This trait abstracts the underlying transport (RESP, gRPC, etc.).
pub trait StreamClient: Send + Sync {
    /// Add entry to stream (XADD)
    fn xadd(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(String, Vec<u8>)],
    ) -> StreamResult<RecordMetadata>;

    /// Read entries from streams (XREAD)
    fn xread(
        &self,
        topics: &[&str],
        ids: &[&str],
        count: Option<u32>,
        block_ms: Option<u64>,
    ) -> StreamResult<Vec<(String, Vec<ConsumerRecord>)>>;

    /// Read with consumer group (XREADGROUP)
    fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        topics: &[&str],
        count: Option<u32>,
        block_ms: Option<u64>,
    ) -> StreamResult<Vec<(String, Vec<ConsumerRecord>)>>;

    /// Acknowledge messages (XACK)
    fn xack(&self, topic: &str, group: &str, ids: &[&str]) -> StreamResult<u32>;

    /// Get pending entries info (XPENDING)
    fn xpending(
        &self,
        topic: &str,
        group: &str,
        start: Option<&str>,
        end: Option<&str>,
        count: Option<u32>,
        consumer: Option<&str>,
    ) -> StreamResult<PendingResult>;

    /// Claim messages (XCLAIM)
    fn xclaim(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        ids: &[&str],
        force: bool,
    ) -> StreamResult<Vec<ConsumerRecord>>;

    /// Auto-claim messages (XAUTOCLAIM)
    fn xautoclaim(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start_id: &str,
        count: u32,
    ) -> StreamResult<AutoClaimResponse>;

    /// Create consumer group (XGROUP CREATE)
    fn xgroup_create(&self, topic: &str, group: &str, start_id: &str) -> StreamResult<()>;

    /// Destroy consumer group (XGROUP DESTROY)
    fn xgroup_destroy(&self, topic: &str, group: &str) -> StreamResult<bool>;

    /// Get stream length (XLEN)
    fn xlen(&self, topic: &str) -> StreamResult<u64>;

    /// Get stream info (XINFO STREAM)
    fn xinfo_stream(&self, topic: &str) -> StreamResult<StreamInfo>;

    /// Get groups info (XINFO GROUPS)
    fn xinfo_groups(&self, topic: &str) -> StreamResult<Vec<GroupInfoResponse>>;

    /// Commit offset for consumer group
    fn commit_offset(&self, group: &str, topic: &str, offset: u64) -> StreamResult<()>;

    /// Get committed offset
    fn get_committed_offset(&self, group: &str, topic: &str) -> StreamResult<Option<u64>>;
}

/// Response from XPENDING command
#[derive(Debug, Clone, Default)]
pub struct PendingResult {
    pub count: u64,
    pub min_id: Option<String>,
    pub max_id: Option<String>,
    pub consumers: std::collections::HashMap<String, u64>,
}

/// Response from XAUTOCLAIM command
#[derive(Debug, Clone, Default)]
pub struct AutoClaimResponse {
    pub next_id: String,
    pub records: Vec<ConsumerRecord>,
    pub deleted_ids: Vec<String>,
}

/// Stream info response
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub length: u64,
    pub first_entry_id: Option<String>,
    pub last_entry_id: Option<String>,
    pub groups: u32,
}

/// Group info response
#[derive(Debug, Clone)]
pub struct GroupInfoResponse {
    pub name: String,
    pub consumers: u32,
    pub pending: u32,
    pub last_delivered_id: String,
}

// ============================================================================
// RESP Client Implementation
// ============================================================================

/// RESP protocol client for SpireDB
///
/// Connects to SpireDB via TCP and sends Redis-compatible stream commands.
pub struct RespClient {
    addr: String,
    conn: Mutex<Option<TcpStream>>,
    timeout: Duration,
}

impl RespClient {
    /// Create a new RESP client
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            conn: Mutex::new(None),
            timeout: Duration::from_secs(30),
        }
    }

    /// Create with custom timeout
    pub fn with_timeout(addr: impl Into<String>, timeout: Duration) -> Self {
        Self {
            addr: addr.into(),
            conn: Mutex::new(None),
            timeout,
        }
    }

    /// Connect to server
    fn connect(&self) -> StreamResult<()> {
        let mut conn = self.conn.lock();

        if conn.is_some() {
            return Ok(());
        }

        let stream = TcpStream::connect(&self.addr).map_err(|e| {
            StreamError::Connection(format!("failed to connect to {}: {}", self.addr, e))
        })?;

        stream.set_read_timeout(Some(self.timeout)).ok();
        stream.set_write_timeout(Some(self.timeout)).ok();

        *conn = Some(stream);
        Ok(())
    }

    /// Send RESP command and get response
    fn send_command(&self, args: &[&[u8]]) -> StreamResult<RespValue> {
        self.connect()?;

        let mut conn = self.conn.lock();

        let stream = conn
            .as_mut()
            .ok_or_else(|| StreamError::Connection("not connected".into()))?;

        // Build RESP array
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n", arg.len()));
        }

        // Write command
        stream
            .write_all(cmd.as_bytes())
            .map_err(|e| StreamError::Connection(format!("write failed: {}", e)))?;

        // Write bulk string data
        for arg in args {
            stream
                .write_all(arg)
                .map_err(|e| StreamError::Connection(format!("write failed: {}", e)))?;
            stream
                .write_all(b"\r\n")
                .map_err(|e| StreamError::Connection(format!("write failed: {}", e)))?;
        }

        stream
            .flush()
            .map_err(|e| StreamError::Connection(format!("flush failed: {}", e)))?;

        // Read response using clone of stream for BufReader
        let stream_clone = stream
            .try_clone()
            .map_err(|e| StreamError::Connection(format!("clone failed: {}", e)))?;
        let mut reader = BufReader::new(stream_clone);
        self.read_resp_value(&mut reader)
    }

    /// Read a RESP value from the stream
    fn read_resp_value<R: BufRead>(&self, reader: &mut R) -> StreamResult<RespValue> {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .map_err(|e| StreamError::Connection(format!("read failed: {}", e)))?;

        if line.is_empty() {
            return Err(StreamError::Connection("connection closed".into()));
        }

        let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

        match line.chars().next() {
            Some('+') => Ok(RespValue::SimpleString(line[1..].to_string())),
            Some('-') => Err(StreamError::Server(line[1..].to_string())),
            Some(':') => {
                let n: i64 = line[1..]
                    .parse()
                    .map_err(|_| StreamError::Protocol("invalid integer".into()))?;
                Ok(RespValue::Integer(n))
            }
            Some('$') => {
                let len: i64 = line[1..]
                    .parse()
                    .map_err(|_| StreamError::Protocol("invalid bulk string length".into()))?;

                if len < 0 {
                    return Ok(RespValue::Null);
                }

                let mut buf = vec![0u8; len as usize];
                reader
                    .read_exact(&mut buf)
                    .map_err(|e| StreamError::Connection(format!("read bulk failed: {}", e)))?;

                // Read trailing \r\n
                let mut crlf = [0u8; 2];
                let _ = reader.read_exact(&mut crlf);

                Ok(RespValue::BulkString(buf))
            }
            Some('*') => {
                let count: i64 = line[1..]
                    .parse()
                    .map_err(|_| StreamError::Protocol("invalid array length".into()))?;

                if count < 0 {
                    return Ok(RespValue::Null);
                }

                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    items.push(self.read_resp_value(reader)?);
                }

                Ok(RespValue::Array(items))
            }
            _ => Err(StreamError::Protocol(format!(
                "unknown RESP type: {}",
                line
            ))),
        }
    }

    /// Parse stream ID from RESP response
    fn parse_stream_id(value: &RespValue) -> Option<String> {
        match value {
            RespValue::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
            RespValue::SimpleString(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// Parse stream entries from XREAD/XREADGROUP response
    fn parse_stream_entries(
        &self,
        value: &RespValue,
    ) -> StreamResult<Vec<(String, Vec<ConsumerRecord>)>> {
        let mut result = Vec::new();

        let streams = match value {
            RespValue::Array(arr) => arr,
            RespValue::Null => return Ok(result),
            _ => return Err(StreamError::Protocol("expected array".into())),
        };

        for stream in streams {
            let stream_arr = match stream {
                RespValue::Array(arr) if arr.len() >= 2 => arr,
                _ => continue,
            };

            let topic = match &stream_arr[0] {
                RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };

            let entries = match &stream_arr[1] {
                RespValue::Array(arr) => arr,
                _ => continue,
            };

            let mut records = Vec::new();
            for entry in entries {
                if let Some(record) = self.parse_entry(&topic, entry) {
                    records.push(record);
                }
            }

            if !records.is_empty() {
                result.push((topic, records));
            }
        }

        Ok(result)
    }

    /// Parse single stream entry
    fn parse_entry(&self, topic: &str, value: &RespValue) -> Option<ConsumerRecord> {
        let arr = match value {
            RespValue::Array(arr) if arr.len() >= 2 => arr,
            _ => return None,
        };

        let id = Self::parse_stream_id(&arr[0])?;
        let fields_arr = match &arr[1] {
            RespValue::Array(arr) => arr,
            _ => return None,
        };

        // Parse fields as key-value pairs
        let mut key: Option<Vec<u8>> = None;
        let mut value_bytes = Vec::new();
        let mut headers = Vec::new();

        let mut i = 0;
        while i + 1 < fields_arr.len() {
            let field_name = match &fields_arr[i] {
                RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                _ => {
                    i += 2;
                    continue;
                }
            };
            let field_value = match &fields_arr[i + 1] {
                RespValue::BulkString(b) => b.clone(),
                _ => Vec::new(),
            };

            match field_name.as_str() {
                "_key" => key = Some(field_value),
                "_value" => value_bytes = field_value,
                name => {
                    headers.push((name.to_string(), field_value));
                }
            }

            i += 2;
        }

        // Parse offset from ID
        let offset: u64 = id.split('-').next_back()?.parse().ok()?;
        let timestamp: u64 = id.split('-').next()?.parse().ok()?;

        Some(ConsumerRecord {
            topic: topic.to_string(),
            partition: 0,
            offset,
            key,
            value: value_bytes,
            headers,
            timestamp,
        })
    }
}

/// RESP protocol value types
#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
}

impl StreamClient for RespClient {
    fn xadd(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        headers: &[(String, Vec<u8>)],
    ) -> StreamResult<RecordMetadata> {
        let mut args: Vec<&[u8]> = vec![b"XADD", topic.as_bytes(), b"*"];

        // Add key if present
        if let Some(k) = key {
            args.push(b"_key");
            args.push(k);
        }

        // Add value
        args.push(b"_value");
        args.push(value);

        // Add headers
        let header_keys: Vec<&[u8]> = headers.iter().map(|(k, _)| k.as_bytes()).collect();
        let header_values: Vec<&[u8]> = headers.iter().map(|(_, v)| v.as_slice()).collect();
        for i in 0..headers.len() {
            args.push(header_keys[i]);
            args.push(header_values[i]);
        }

        let response = self.send_command(&args)?;

        let id = Self::parse_stream_id(&response)
            .ok_or_else(|| StreamError::Protocol("invalid XADD response".into()))?;

        let parts: Vec<&str> = id.split('-').collect();
        let timestamp: u64 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
        let offset: u64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        Ok(RecordMetadata {
            topic: topic.to_string(),
            partition: 0,
            offset,
            timestamp,
        })
    }

    fn xread(
        &self,
        topics: &[&str],
        ids: &[&str],
        count: Option<u32>,
        block_ms: Option<u64>,
    ) -> StreamResult<Vec<(String, Vec<ConsumerRecord>)>> {
        let mut args: Vec<Vec<u8>> = vec![b"XREAD".to_vec()];

        if let Some(c) = count {
            args.push(b"COUNT".to_vec());
            args.push(c.to_string().into_bytes());
        }

        if let Some(b) = block_ms {
            args.push(b"BLOCK".to_vec());
            args.push(b.to_string().into_bytes());
        }

        args.push(b"STREAMS".to_vec());
        for topic in topics {
            args.push(topic.as_bytes().to_vec());
        }
        for id in ids {
            args.push(id.as_bytes().to_vec());
        }

        let arg_refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        let response = self.send_command(&arg_refs)?;

        self.parse_stream_entries(&response)
    }

    fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        topics: &[&str],
        count: Option<u32>,
        block_ms: Option<u64>,
    ) -> StreamResult<Vec<(String, Vec<ConsumerRecord>)>> {
        let mut args: Vec<Vec<u8>> = vec![
            b"XREADGROUP".to_vec(),
            b"GROUP".to_vec(),
            group.as_bytes().to_vec(),
            consumer.as_bytes().to_vec(),
        ];

        if let Some(c) = count {
            args.push(b"COUNT".to_vec());
            args.push(c.to_string().into_bytes());
        }

        if let Some(b) = block_ms {
            args.push(b"BLOCK".to_vec());
            args.push(b.to_string().into_bytes());
        }

        args.push(b"STREAMS".to_vec());
        for topic in topics {
            args.push(topic.as_bytes().to_vec());
        }
        for _ in topics {
            args.push(b">".to_vec()); // Read new messages
        }

        let arg_refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        let response = self.send_command(&arg_refs)?;

        self.parse_stream_entries(&response)
    }

    fn xack(&self, topic: &str, group: &str, ids: &[&str]) -> StreamResult<u32> {
        let mut args: Vec<&[u8]> = vec![b"XACK", topic.as_bytes(), group.as_bytes()];
        for id in ids {
            args.push(id.as_bytes());
        }

        let response = self.send_command(&args)?;

        match response {
            RespValue::Integer(n) => Ok(n as u32),
            _ => Err(StreamError::Protocol("expected integer".into())),
        }
    }

    fn xpending(
        &self,
        topic: &str,
        group: &str,
        start: Option<&str>,
        end: Option<&str>,
        count: Option<u32>,
        consumer: Option<&str>,
    ) -> StreamResult<PendingResult> {
        let mut args: Vec<Vec<u8>> = vec![
            b"XPENDING".to_vec(),
            topic.as_bytes().to_vec(),
            group.as_bytes().to_vec(),
        ];

        if let (Some(s), Some(e), Some(c)) = (start, end, count) {
            args.push(s.as_bytes().to_vec());
            args.push(e.as_bytes().to_vec());
            args.push(c.to_string().into_bytes());

            if let Some(cons) = consumer {
                args.push(cons.as_bytes().to_vec());
            }
        }

        let arg_refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        let response = self.send_command(&arg_refs)?;

        // Parse summary format: [count, min-id, max-id, [[consumer, count], ...]]
        match response {
            RespValue::Array(arr) if arr.len() >= 4 => {
                let count = match &arr[0] {
                    RespValue::Integer(n) => *n as u64,
                    _ => 0,
                };
                let min_id = Self::parse_stream_id(&arr[1]);
                let max_id = Self::parse_stream_id(&arr[2]);

                let mut consumers = std::collections::HashMap::new();
                if let RespValue::Array(cons_arr) = &arr[3] {
                    for c in cons_arr {
                        if let RespValue::Array(pair) = c
                            && pair.len() >= 2
                        {
                            let name = Self::parse_stream_id(&pair[0]).unwrap_or_default();
                            let cnt = match &pair[1] {
                                RespValue::BulkString(b) => {
                                    String::from_utf8_lossy(b).parse().unwrap_or(0)
                                }
                                RespValue::Integer(n) => *n as u64,
                                _ => 0,
                            };
                            consumers.insert(name, cnt);
                        }
                    }
                }

                Ok(PendingResult {
                    count,
                    min_id,
                    max_id,
                    consumers,
                })
            }
            _ => Ok(PendingResult::default()),
        }
    }

    fn xclaim(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        ids: &[&str],
        force: bool,
    ) -> StreamResult<Vec<ConsumerRecord>> {
        let min_idle_str = min_idle_ms.to_string();
        let mut args: Vec<&[u8]> = vec![
            b"XCLAIM",
            topic.as_bytes(),
            group.as_bytes(),
            consumer.as_bytes(),
            min_idle_str.as_bytes(),
        ];

        for id in ids {
            args.push(id.as_bytes());
        }

        if force {
            args.push(b"FORCE");
        }

        let response = self.send_command(&args)?;

        // Parse claimed entries
        match response {
            RespValue::Array(entries) => {
                let mut records = Vec::new();
                for entry in &entries {
                    if let Some(record) = self.parse_entry(topic, entry) {
                        records.push(record);
                    }
                }
                Ok(records)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn xautoclaim(
        &self,
        topic: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: u64,
        start_id: &str,
        count: u32,
    ) -> StreamResult<AutoClaimResponse> {
        let min_idle_str = min_idle_ms.to_string();
        let count_str = count.to_string();

        let args: Vec<&[u8]> = vec![
            b"XAUTOCLAIM",
            topic.as_bytes(),
            group.as_bytes(),
            consumer.as_bytes(),
            min_idle_str.as_bytes(),
            start_id.as_bytes(),
            b"COUNT",
            count_str.as_bytes(),
        ];

        let response = self.send_command(&args)?;

        // Response: [next-id, [entries...], [deleted-ids...]]
        match response {
            RespValue::Array(arr) if arr.len() >= 2 => {
                let next_id = Self::parse_stream_id(&arr[0]).unwrap_or_else(|| "0-0".to_string());

                let mut records = Vec::new();
                if let RespValue::Array(entries) = &arr[1] {
                    for entry in entries {
                        if let Some(record) = self.parse_entry(topic, entry) {
                            records.push(record);
                        }
                    }
                }

                let mut deleted_ids = Vec::new();
                if arr.len() >= 3
                    && let RespValue::Array(deleted) = &arr[2]
                {
                    for d in deleted {
                        if let Some(id) = Self::parse_stream_id(d) {
                            deleted_ids.push(id);
                        }
                    }
                }

                Ok(AutoClaimResponse {
                    next_id,
                    records,
                    deleted_ids,
                })
            }
            _ => Ok(AutoClaimResponse::default()),
        }
    }

    fn xgroup_create(&self, topic: &str, group: &str, start_id: &str) -> StreamResult<()> {
        let args: Vec<&[u8]> = vec![
            b"XGROUP",
            b"CREATE",
            topic.as_bytes(),
            group.as_bytes(),
            start_id.as_bytes(),
            b"MKSTREAM",
        ];

        match self.send_command(&args) {
            Ok(_) => Ok(()),
            Err(StreamError::Server(msg)) if msg.contains("BUSYGROUP") => Ok(()), // Group exists
            Err(e) => Err(e),
        }
    }

    fn xgroup_destroy(&self, topic: &str, group: &str) -> StreamResult<bool> {
        let args: Vec<&[u8]> = vec![b"XGROUP", b"DESTROY", topic.as_bytes(), group.as_bytes()];

        let response = self.send_command(&args)?;

        match response {
            RespValue::Integer(n) => Ok(n > 0),
            _ => Ok(false),
        }
    }

    fn xlen(&self, topic: &str) -> StreamResult<u64> {
        let args: Vec<&[u8]> = vec![b"XLEN", topic.as_bytes()];
        let response = self.send_command(&args)?;

        match response {
            RespValue::Integer(n) => Ok(n as u64),
            _ => Err(StreamError::Protocol("expected integer".into())),
        }
    }

    fn xinfo_stream(&self, topic: &str) -> StreamResult<StreamInfo> {
        let args: Vec<&[u8]> = vec![b"XINFO", b"STREAM", topic.as_bytes()];
        let response = self.send_command(&args)?;

        // Parse XINFO STREAM response (array of key-value pairs)
        let mut length = 0u64;
        let mut first_entry_id = None;
        let mut last_entry_id = None;
        let mut groups = 0u32;

        if let RespValue::Array(arr) = response {
            let mut i = 0;
            while i + 1 < arr.len() {
                let key = match &arr[i] {
                    RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                    _ => {
                        i += 2;
                        continue;
                    }
                };

                match key.as_str() {
                    "length" => {
                        if let RespValue::Integer(n) = &arr[i + 1] {
                            length = *n as u64;
                        }
                    }
                    "first-entry" => {
                        if let RespValue::Array(entry) = &arr[i + 1]
                            && !entry.is_empty()
                        {
                            first_entry_id = Self::parse_stream_id(&entry[0]);
                        }
                    }
                    "last-entry" => {
                        if let RespValue::Array(entry) = &arr[i + 1]
                            && !entry.is_empty()
                        {
                            last_entry_id = Self::parse_stream_id(&entry[0]);
                        }
                    }
                    "groups" => {
                        if let RespValue::Integer(n) = &arr[i + 1] {
                            groups = *n as u32;
                        }
                    }
                    _ => {}
                }
                i += 2;
            }
        }

        Ok(StreamInfo {
            length,
            first_entry_id,
            last_entry_id,
            groups,
        })
    }

    fn xinfo_groups(&self, topic: &str) -> StreamResult<Vec<GroupInfoResponse>> {
        let args: Vec<&[u8]> = vec![b"XINFO", b"GROUPS", topic.as_bytes()];
        let response = self.send_command(&args)?;

        let mut result = Vec::new();

        if let RespValue::Array(groups) = response {
            for group in groups {
                if let RespValue::Array(arr) = group {
                    let mut name = String::new();
                    let mut consumers = 0u32;
                    let mut pending = 0u32;
                    let mut last_delivered_id = String::new();

                    let mut i = 0;
                    while i + 1 < arr.len() {
                        let key = match &arr[i] {
                            RespValue::BulkString(b) => String::from_utf8_lossy(b).to_string(),
                            _ => {
                                i += 2;
                                continue;
                            }
                        };

                        match key.as_str() {
                            "name" => {
                                name = Self::parse_stream_id(&arr[i + 1]).unwrap_or_default();
                            }
                            "consumers" => {
                                if let RespValue::Integer(n) = &arr[i + 1] {
                                    consumers = *n as u32;
                                }
                            }
                            "pending" => {
                                if let RespValue::Integer(n) = &arr[i + 1] {
                                    pending = *n as u32;
                                }
                            }
                            "last-delivered-id" => {
                                last_delivered_id =
                                    Self::parse_stream_id(&arr[i + 1]).unwrap_or_default();
                            }
                            _ => {}
                        }
                        i += 2;
                    }

                    result.push(GroupInfoResponse {
                        name,
                        consumers,
                        pending,
                        last_delivered_id,
                    });
                }
            }
        }

        Ok(result)
    }

    fn commit_offset(&self, group: &str, topic: &str, offset: u64) -> StreamResult<()> {
        // Use HSET to store committed offset
        let key = format!("__consumer_offsets:{}:{}", group, topic);
        let offset_str = offset.to_string();
        let args: Vec<&[u8]> = vec![b"HSET", key.as_bytes(), b"offset", offset_str.as_bytes()];

        self.send_command(&args)?;
        Ok(())
    }

    fn get_committed_offset(&self, group: &str, topic: &str) -> StreamResult<Option<u64>> {
        let key = format!("__consumer_offsets:{}:{}", group, topic);
        let args: Vec<&[u8]> = vec![b"HGET", key.as_bytes(), b"offset"];

        let response = self.send_command(&args)?;

        match response {
            RespValue::BulkString(b) => {
                let s = String::from_utf8_lossy(&b);
                Ok(s.parse().ok())
            }
            RespValue::Null => Ok(None),
            _ => Ok(None),
        }
    }
}
