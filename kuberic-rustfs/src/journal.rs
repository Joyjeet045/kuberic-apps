use std::path::Path;

use anyhow::{Context, Result, ensure};
use kuberic_native_runtime::native::{NativeOperation, NativeOperationStatus};
use rusqlite::{Connection, OptionalExtension, params};

use crate::Topology;

pub(crate) struct Journal(Connection);

impl Journal {
    pub(crate) fn open(path: &Path, bootstrap: &Topology) -> Result<Self> {
        let mut connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS settings (
                 id INTEGER PRIMARY KEY CHECK (id = 1), bootstrap TEXT NOT NULL, topology TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS operations (
                 id TEXT PRIMARY KEY, request TEXT NOT NULL, evidence TEXT, launched INTEGER NOT NULL DEFAULT 0
             );
             CREATE UNIQUE INDEX IF NOT EXISTS one_pending ON operations ((1)) WHERE evidence IS NULL;",
        )?;
        let transaction = connection.transaction()?;
        let stored: Option<String> = transaction
            .query_row("SELECT bootstrap FROM settings WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        if let Some(stored) = stored {
            ensure!(
                serde_json::from_str::<Topology>(&stored)? == *bootstrap,
                "adapter bootstrap identity does not match persisted storage"
            );
        } else {
            let value = serde_json::to_string(bootstrap)?;
            transaction.execute("INSERT INTO settings VALUES (1, ?1, ?1)", [value])?;
        }
        transaction.commit()?;
        Ok(Self(connection))
    }

    pub(crate) fn topology(&self) -> Result<Topology> {
        let value: String =
            self.0
                .query_row("SELECT topology FROM settings WHERE id = 1", [], |row| {
                    row.get(0)
                })?;
        serde_json::from_str(&value).context("corrupt adapter topology journal")
    }

    pub(crate) fn pending(&self) -> Result<Option<NativeOperation>> {
        self.0
            .query_row(
                "SELECT id, request FROM operations WHERE evidence IS NULL",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .map(|(id, request)| {
                Ok(NativeOperation {
                    id,
                    request: serde_json::from_str(&request)?,
                })
            })
            .transpose()
    }

    pub(crate) fn lookup(
        &self,
        operation: &NativeOperation,
    ) -> Result<Option<NativeOperationStatus>> {
        let existing: Option<(String, Option<String>)> = self
            .0
            .query_row(
                "SELECT request, evidence FROM operations WHERE id = ?1",
                [&operation.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        existing
            .map(|(request, evidence)| {
                ensure!(
                    serde_json::from_str::<serde_json::Value>(&request)? == operation.request,
                    "operation id cannot be reused with different input"
                );
                evidence.map_or(Ok(NativeOperationStatus::Pending), |value| {
                    Ok(NativeOperationStatus::Complete {
                        evidence: serde_json::from_str(&value)?,
                    })
                })
            })
            .transpose()
    }

    pub(crate) fn launched(&self, id: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT launched FROM operations WHERE id = ?1",
            [id],
            |row| row.get(0),
        )?)
    }

    pub(crate) fn completed_operation(
        &self,
        id: &str,
    ) -> Result<(NativeOperation, serde_json::Value)> {
        let (request, evidence): (String, String) = self
            .0
            .query_row(
                "SELECT request, evidence FROM operations WHERE id = ?1 AND evidence IS NOT NULL",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .context("pool retirement requires a durable local completion receipt")?;
        Ok((
            NativeOperation {
                id: id.to_owned(),
                request: serde_json::from_str(&request)?,
            },
            serde_json::from_str(&evidence)?,
        ))
    }

    pub(crate) fn record_launch(&self, id: &str) -> Result<()> {
        ensure!(
            self.0
                .execute("UPDATE operations SET launched = 1 WHERE id = ?1", [id])?
                == 1,
            "missing native operation"
        );
        Ok(())
    }

    pub(crate) fn accept(
        &mut self,
        operation: &NativeOperation,
        topology: Option<&Topology>,
    ) -> Result<NativeOperationStatus> {
        ensure!(
            !operation.id.is_empty()
                && operation.id.len() <= 128
                && operation
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
            "operation id must contain 1-128 ASCII identifier characters"
        );
        let transaction = self.0.transaction()?;
        let existing: Option<(String, Option<String>)> = transaction
            .query_row(
                "SELECT request, evidence FROM operations WHERE id = ?1",
                [&operation.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((request, evidence)) = existing {
            ensure!(
                serde_json::from_str::<serde_json::Value>(&request)? == operation.request,
                "operation id cannot be reused with different input"
            );
            return evidence.map_or(Ok(NativeOperationStatus::Pending), |value| {
                Ok(NativeOperationStatus::Complete {
                    evidence: serde_json::from_str(&value)?,
                })
            });
        }
        transaction
            .execute(
                "INSERT INTO operations (id, request) VALUES (?1, ?2)",
                params![operation.id, serde_json::to_string(&operation.request)?],
            )
            .context("another native operation is pending")?;
        if let Some(topology) = topology {
            transaction.execute(
                "UPDATE settings SET topology = ?1 WHERE id = 1",
                [serde_json::to_string(topology)?],
            )?;
        }
        transaction.commit()?;
        Ok(NativeOperationStatus::Pending)
    }

    pub(crate) fn complete(&mut self, id: &str, evidence: &serde_json::Value) -> Result<()> {
        ensure!(
            self.0.execute(
                "UPDATE operations SET evidence = ?1 WHERE id = ?2 AND evidence IS NULL",
                params![serde_json::to_string(evidence)?, id],
            )? == 1,
            "native completion does not match a pending operation"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn identity_conflicts_and_pending_work_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal.db");
        let topology = Topology {
            pools: vec!["initial".into()],
            local_node: None,
            erasure_set_drive_count: None,
        };
        let operation = NativeOperation {
            id: "expand-1".into(),
            request: json!({"target": "pool"}),
        };
        let mut journal = Journal::open(&path, &topology).unwrap();
        journal.accept(&operation, Some(&topology)).unwrap();
        drop(journal);
        let mut journal = Journal::open(&path, &topology).unwrap();
        assert_eq!(journal.pending().unwrap(), Some(operation.clone()));
        assert_eq!(journal.topology().unwrap(), topology);
        assert!(
            journal
                .accept(
                    &NativeOperation {
                        id: operation.id.clone(),
                        request: json!(null)
                    },
                    None
                )
                .is_err()
        );
        assert!(
            journal
                .accept(
                    &NativeOperation {
                        id: "second".into(),
                        ..operation.clone()
                    },
                    None
                )
                .is_err()
        );
        journal
            .complete(&operation.id, &json!({"native": "complete"}))
            .unwrap();
        drop(journal);
        let mut journal = Journal::open(&path, &topology).unwrap();
        assert_eq!(journal.pending().unwrap(), None);
        assert_eq!(
            journal.accept(&operation, None).unwrap(),
            NativeOperationStatus::Complete {
                evidence: json!({"native": "complete"})
            }
        );
        let different = Topology {
            pools: vec!["other".into()],
            ..topology
        };
        assert!(Journal::open(&path, &different).is_err());
    }
}
