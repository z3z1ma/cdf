use std::{path::PathBuf, sync::Arc};

use cdf_kernel::{CdfError, Result};
use cdf_runtime::{SpillBudgetCoordinator, SpillReservation};
use cdf_task_store::{ExternalTaskStore, ExternalTaskWorkspace};
use rusqlite::{Connection, OptionalExtension, params};

use crate::GlueObjectTask;

pub struct GluePlanningIndex {
    connection: Connection,
    path: PathBuf,
    _workspace: ExternalTaskWorkspace,
    reservation: SpillReservation,
    growth_bytes: u64,
    object_count: u64,
    estimated_bytes: u64,
    transaction_open: bool,
}

impl GluePlanningIndex {
    pub fn create(
        store: &ExternalTaskStore,
        spill: Arc<dyn SpillBudgetCoordinator>,
        growth_bytes: u64,
    ) -> Result<Self> {
        if growth_bytes < 8192 {
            return Err(CdfError::contract(
                "Glue planning spill growth must be at least 8192 bytes",
            ));
        }
        let workspace = store.temporary_workspace("glue-planning")?;
        let path = workspace.path().join("objects.sqlite");
        let reservation = spill.try_reserve(growth_bytes)?.ok_or_else(|| {
            CdfError::data(format!(
                "Glue canonical planning requires {growth_bytes} spill bytes but the shared disk budget is exhausted"
            ))
        })?;
        let connection = Connection::open(&path)
            .map_err(|error| CdfError::data(format!("open Glue planning index: {error}")))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=OFF;
                 PRAGMA synchronous=OFF;
                 PRAGMA temp_store=FILE;
                 CREATE TABLE objects (
                    path TEXT PRIMARY KEY NOT NULL,
                    payload BLOB NOT NULL,
                    estimated_bytes INTEGER NOT NULL
                 ) WITHOUT ROWID;",
            )
            .map_err(|error| CdfError::data(format!("initialize Glue planning index: {error}")))?;
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|error| CdfError::data(format!("begin Glue planning transaction: {error}")))?;
        Ok(Self {
            connection,
            path,
            _workspace: workspace,
            reservation,
            growth_bytes,
            object_count: 0,
            estimated_bytes: 0,
            transaction_open: true,
        })
    }

    pub fn insert(&mut self, task: &GlueObjectTask) -> Result<()> {
        let payload = serde_json::to_vec(task)
            .map_err(|error| CdfError::data(format!("encode Glue planning task: {error}")))?;
        let prospective = self
            .path
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or(0)
            .saturating_add(
                u64::try_from(payload.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(4)
                    .saturating_add(8192),
            );
        self.ensure_reserved(prospective)?;
        let existing = self
            .connection
            .query_row(
                "SELECT payload FROM objects WHERE path = ?1",
                [&task.file.path],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(|error| CdfError::data(format!("query Glue planning object: {error}")))?;
        if let Some(existing) = existing {
            if existing != payload {
                return Err(CdfError::data(format!(
                    "Glue planning observed conflicting descriptors for object `{}`",
                    task.file.path
                )));
            }
            return Ok(());
        }
        let bytes = i64::try_from(task.file.size_bytes)
            .map_err(|_| CdfError::data("Glue object byte count exceeds SQLite i64"))?;
        self.connection
            .execute(
                "INSERT INTO objects(path, payload, estimated_bytes) VALUES (?1, ?2, ?3)",
                params![task.file.path, payload, bytes],
            )
            .map_err(|error| CdfError::data(format!("insert Glue planning object: {error}")))?;
        self.object_count = self
            .object_count
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Glue planning object count overflowed"))?;
        self.estimated_bytes = self
            .estimated_bytes
            .checked_add(task.file.size_bytes)
            .ok_or_else(|| CdfError::data("Glue planning byte estimate overflowed"))?;
        self.ensure_reserved(self.path.metadata().map(|m| m.len()).unwrap_or(0))
    }

    pub fn object_count(&self) -> Result<u64> {
        Ok(self.object_count)
    }

    pub fn estimated_bytes(&self) -> Result<u64> {
        Ok(self.estimated_bytes)
    }

    pub fn for_each_canonical(
        &mut self,
        mut visit: impl FnMut(u64, GlueObjectTask) -> Result<()>,
    ) -> Result<()> {
        if self.transaction_open {
            self.connection
                .execute_batch("COMMIT")
                .map_err(|error| CdfError::data(format!("commit Glue planning index: {error}")))?;
            self.transaction_open = false;
        }
        self.ensure_reserved(self.path.metadata().map(|m| m.len()).unwrap_or(0))?;
        let mut statement = self
            .connection
            .prepare("SELECT payload FROM objects ORDER BY path")
            .map_err(|error| CdfError::data(format!("prepare Glue planning scan: {error}")))?;
        let mut rows = statement
            .query([])
            .map_err(|error| CdfError::data(format!("open Glue planning scan: {error}")))?;
        let mut ordinal = 0_u64;
        while let Some(row) = rows
            .next()
            .map_err(|error| CdfError::data(format!("read Glue planning task: {error}")))?
        {
            let payload: Vec<u8> = row
                .get(0)
                .map_err(|error| CdfError::data(format!("decode Glue planning row: {error}")))?;
            let mut task: GlueObjectTask = serde_json::from_slice(&payload)
                .map_err(|error| CdfError::data(format!("parse Glue planning task: {error}")))?;
            task.canonical_ordinal = ordinal;
            visit(ordinal, task)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| CdfError::data("Glue planning ordinal overflowed"))?;
        }
        Ok(())
    }

    fn ensure_reserved(&mut self, required: u64) -> Result<()> {
        if required <= self.reservation.bytes() {
            return Ok(());
        }
        let additional = required
            .saturating_sub(self.reservation.bytes())
            .div_ceil(self.growth_bytes)
            .saturating_mul(self.growth_bytes);
        if !self.reservation.try_grow(additional)? {
            return Err(CdfError::data(format!(
                "Glue planning index requires {} spill bytes but the shared disk budget is exhausted",
                self.reservation.bytes().saturating_add(additional)
            )));
        }
        Ok(())
    }
}
