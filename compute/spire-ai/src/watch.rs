//! CDC-based change watching for collections.

use std::marker::PhantomData;

use spiresql::stream::cdc::{CdcBuilder, CdcStream};
use spiresql::stream::types::Op;

use crate::document::Doc;
use crate::error::{Error, Result};

/// A change event on a document in a collection.
#[derive(Debug, Clone)]
pub struct Change<T> {
    /// Document ID
    pub id: String,
    /// Operation type (Insert, Update, Delete)
    pub op: Op,
    /// Document state before the change (for Update/Delete)
    pub before: Option<T>,
    /// Document state after the change (for Insert/Update)
    pub after: Option<T>,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,
}

/// A stream of typed changes to a collection, backed by SpireDB CDC.
pub struct WatchStream<T: Doc> {
    inner: CdcStream,
    _phantom: PhantomData<T>,
}

impl<T: Doc> WatchStream<T> {
    pub(crate) async fn new(stream_addr: &str, table_name: &str) -> Result<Self> {
        let cdc = CdcBuilder::new(stream_addr)
            .tables(&[table_name])
            .operations(&[Op::Insert, Op::Update, Op::Delete])
            .current()
            .build()
            .await
            .map_err(Error::Stream)?;

        Ok(Self {
            inner: cdc,
            _phantom: PhantomData,
        })
    }

    /// Poll for the next change event.
    pub async fn next(&self) -> Result<Option<Change<T>>> {
        let event = self.inner.poll().await.map_err(Error::Stream)?;

        match event {
            None => Ok(None),
            Some(event) => {
                let before = event.before.and_then(|v| serde_json::from_value(v).ok());
                let after = event.after.and_then(|v| serde_json::from_value(v).ok());

                // Extract ID from the after state (insert/update) or before state (delete)
                let id = after
                    .as_ref()
                    .map(|d: &T| d.id().to_string())
                    .or_else(|| before.as_ref().map(|d: &T| d.id().to_string()))
                    .unwrap_or_default();

                Ok(Some(Change {
                    id,
                    op: event.op,
                    before,
                    after,
                    timestamp: event.timestamp,
                }))
            }
        }
    }

    /// Close the watch stream.
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await.map_err(Error::Stream)
    }
}
