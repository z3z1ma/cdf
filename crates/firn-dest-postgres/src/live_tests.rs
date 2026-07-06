use std::{
    env,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use firn_kernel::{
    CheckpointId, CursorPosition, CursorValue, PartitionId, PipelineId, ScopeKey, SegmentId,
    SourcePosition,
};
use firn_package::{PackageBuilder, PackageManifest};
use postgres::{Client, NoTls};
use tempfile::TempDir;

use super::*;
use crate::{ddl::target_migrations, identifiers::quote_identifier_unchecked};

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

struct LivePostgres {
    url: String,
    schema: String,
    _server: Option<LocalPostgres>,
}

struct LocalPostgres {
    data_dir: TempDir,
    _socket_dir: TempDir,
    pg_ctl: PathBuf,
}

impl LivePostgres {
    fn start() -> Option<Self> {
        let (url, server) = match env::var("TEST_DATABASE_URL") {
            Ok(url) if !url.trim().is_empty() => (url, None),
            _ => {
                let Some(server) = LocalPostgres::start() else {
                    eprintln!(
                        "skipping live Postgres test: set TEST_DATABASE_URL or install postgres/initdb/pg_ctl"
                    );
                    return None;
                };
                (server.url(), Some(server))
            }
        };
        let schema = format!(
            "firn_live_{}_{}",
            std::process::id(),
            SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let mut client = Client::connect(&url, NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE SCHEMA {}",
                quote_identifier(&schema).unwrap()
            ))
            .unwrap();
        Some(Self {
            url,
            schema,
            _server: server,
        })
    }

    fn destination(&self) -> PostgresDestination {
        PostgresDestination::connect(self.url.clone()).unwrap()
    }

    fn client(&self) -> Client {
        let mut client = Client::connect(&self.url, NoTls).unwrap();
        client
            .batch_execute(&format!(
                "SET search_path = {}, public",
                quote_identifier(&self.schema).unwrap()
            ))
            .unwrap();
        client
    }

    fn target(&self, table: &str) -> PostgresTarget {
        PostgresTarget::new(Some(&self.schema), table).unwrap()
    }
}

impl Drop for LivePostgres {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.url, NoTls) {
            let _ = client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_identifier(&self.schema).unwrap()
            ));
        }
    }
}

impl LocalPostgres {
    fn start() -> Option<Self> {
        let initdb = find_binary("initdb")?;
        let pg_ctl = find_binary("pg_ctl")?;
        let data_dir = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let port = free_port();

        let init_status = Command::new(&initdb)
            .args(["-D", data_dir.path().to_str().unwrap()])
            .args(["-A", "trust"])
            .args(["-U", "firn"])
            .arg("--no-sync")
            .status()
            .unwrap();
        assert!(init_status.success(), "initdb failed");

        let options = format!("-h 127.0.0.1 -p {port} -k {}", socket_dir.path().display());
        let log_path = data_dir.path().join("postgres.log");
        let start_status = Command::new(&pg_ctl)
            .args(["-D", data_dir.path().to_str().unwrap()])
            .args(["-l", log_path.to_str().unwrap()])
            .args(["-o", &options])
            .args(["-w", "start"])
            .status()
            .unwrap();
        assert!(start_status.success(), "pg_ctl start failed");

        Some(Self {
            data_dir,
            _socket_dir: socket_dir,
            pg_ctl,
        })
    }

    fn url(&self) -> String {
        let port = std::fs::read_to_string(self.data_dir.path().join("postmaster.pid"))
            .unwrap()
            .lines()
            .nth(3)
            .unwrap()
            .to_owned();
        format!("postgresql://firn@127.0.0.1:{port}/postgres")
    }
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        let _ = Command::new(&self.pg_ctl)
            .args(["-D", self.data_dir.path().to_str().unwrap()])
            .args(["-m", "fast"])
            .args(["-w", "stop"])
            .status();
    }
}

fn find_binary(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|path| path.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef =
        std::sync::Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id)));
    let name: ArrayRef =
        std::sync::Arc::new(StringArray::from_iter(rows.iter().map(|(_, name)| *name)));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn build_package(
    root: &Path,
    package_id: &str,
    segments: Vec<(&str, RecordBatch)>,
) -> PackageManifest {
    let mut builder = PackageBuilder::create(root, package_id).unwrap();
    for (segment_id, batch) in segments {
        builder
            .write_segment(SegmentId::new(segment_id).unwrap(), &[batch])
            .unwrap();
    }
    builder.finish().unwrap()
}

fn columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn::new("id", "BIGINT", false).unwrap(),
        PostgresColumn::new("name", "TEXT", true).unwrap(),
    ]
}

fn decimal_columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn::new("id", "BIGINT", false).unwrap(),
        PostgresColumn::new("amount", "NUMERIC(12,2)", true).unwrap(),
    ]
}

fn decimal_batch(rows: &[(i64, Option<i128>)]) -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(12, 2), true),
    ]));
    let id: ArrayRef =
        std::sync::Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id)));
    let amount: ArrayRef = std::sync::Arc::new(
        Decimal128Array::from(rows.iter().map(|(_, amount)| *amount).collect::<Vec<_>>())
            .with_precision_and_scale(12, 2)
            .unwrap(),
    );
    RecordBatch::try_new(schema, vec![id, amount]).unwrap()
}

fn state_segments(manifest: &PackageManifest) -> Vec<StateSegment> {
    manifest
        .identity
        .segments
        .iter()
        .map(|segment| StateSegment {
            segment_id: segment.segment_id.clone(),
            scope: scope(),
            output_position: position(10),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect()
}

fn state_delta(manifest: &PackageManifest, checkpoint: &str) -> StateDelta {
    StateDelta {
        checkpoint_id: CheckpointId::new(checkpoint).unwrap(),
        pipeline_id: PipelineId::new("pipe-live").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope: scope(),
        state_version: 1,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: position(10),
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        schema_hash: schema_hash(),
        segments: state_segments(manifest),
    }
}

fn scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    }
}

fn position(value: i64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(value),
    })
}

fn schema_hash() -> SchemaHash {
    SchemaHash::new("sha256:live-schema").unwrap()
}

fn plan(
    env: &LivePostgres,
    table: &str,
    manifest: &PackageManifest,
    disposition: WriteDisposition,
    dedup: MergeDedupPolicy,
    state_delta: Option<StateDelta>,
) -> PostgresLoadPlan {
    plan_with_columns(
        env,
        table,
        manifest,
        disposition,
        dedup,
        state_delta,
        columns(),
    )
}

fn plan_with_columns(
    env: &LivePostgres,
    table: &str,
    manifest: &PackageManifest,
    disposition: WriteDisposition,
    dedup: MergeDedupPolicy,
    state_delta: Option<StateDelta>,
    columns: Vec<PostgresColumn>,
) -> PostgresLoadPlan {
    env.destination()
        .plan_load(load_input(
            env,
            table,
            manifest,
            disposition,
            dedup,
            state_delta,
            columns,
        ))
        .unwrap()
}

fn load_input(
    env: &LivePostgres,
    table: &str,
    manifest: &PackageManifest,
    disposition: WriteDisposition,
    dedup: MergeDedupPolicy,
    state_delta: Option<StateDelta>,
    columns: Vec<PostgresColumn>,
) -> PostgresLoadPlanInput {
    PostgresLoadPlanInput {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        idempotency_token: IdempotencyToken::new(manifest.package_hash.clone()).unwrap(),
        target: env.target(table),
        disposition,
        schema_hash: schema_hash(),
        segments: state_segments(manifest),
        columns,
        merge_keys: vec![PostgresIdentifier::user("id").unwrap()],
        dedup,
        existing_table: None,
        resource_id: Some(ResourceId::new("orders").unwrap()),
        state_delta,
    }
}

fn commit(env: &LivePostgres, package_dir: &Path, plan: PostgresLoadPlan) -> PostgresCommitOutcome {
    env.destination()
        .commit_package(PostgresCommitRequest {
            package_dir: package_dir.to_path_buf(),
            plan,
        })
        .unwrap()
}

#[test]
fn live_append_duplicate_receipt_verification_and_state_mirror() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-append",
        vec![
            ("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))])),
            ("seg-000002", batch(&[(3, None)])),
        ],
    );
    let plan = plan(
        &env,
        "orders_append",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-append")),
    );

    let outcome = commit(&env, package_dir.path(), plan.clone());
    assert!(!outcome.duplicate);
    assert!(outcome.package_receipt_recorded);
    assert_eq!(outcome.receipt.counts.rows_written, 3);
    assert_eq!(outcome.receipt.segment_acks.len(), 2);
    assert_eq!(outcome.receipt.schema_hash, schema_hash());
    assert_eq!(outcome.receipt.verify.kind, "postgres_sql");
    assert!(
        outcome
            .receipt
            .transaction
            .as_ref()
            .unwrap()
            .values
            .get("xid")
            .unwrap()
            .parse::<u64>()
            .is_ok()
    );
    assert!(
        env.destination()
            .verify_receipt(&outcome.receipt)
            .unwrap()
            .verified
    );

    let mut client = env.client();
    let target = env.target("orders_append");
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"name\", \"_firn_load\", \"_firn_segment\", \"_firn_row\", \"_firn_loaded_at_ms\" FROM {} ORDER BY \"id\"",
                target.sql()
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "ada");
    assert_eq!(rows[0].get::<_, String>(2), manifest.package_hash);
    assert_eq!(rows[0].get::<_, String>(3), "seg-000001");
    assert_eq!(rows[0].get::<_, i64>(4), 0);
    assert!(rows[0].get::<_, i64>(5) > 0);

    let load_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
                quote_identifier_unchecked(FIRN_LOADS_TABLE)
            ),
            &[&target.display_name(), &manifest.package_hash],
        )
        .unwrap()
        .get(0);
    assert_eq!(load_count, 1);

    let schema_hash_value = schema_hash();
    let state_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"package_hash\" = $1 AND \"schema_hash\" = $2",
                quote_identifier_unchecked(FIRN_STATE_TABLE)
            ),
            &[&manifest.package_hash, &schema_hash_value.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(state_count, 1);

    let duplicate = commit(&env, package_dir.path(), plan);
    assert!(duplicate.duplicate);
    assert!(!duplicate.package_receipt_recorded);
    assert!(duplicate.package_receipt_error.is_none());
    assert_eq!(duplicate.receipt.receipt_id, outcome.receipt.receipt_id);
    let target_count: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::bigint FROM {}", target.sql()),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(target_count, 3);

    let mut different_token_input = load_input(
        &env,
        "orders_append",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-append")),
        columns(),
    );
    different_token_input.idempotency_token =
        IdempotencyToken::new("token-different-but-same-package").unwrap();
    let duplicate_with_different_token = commit(
        &env,
        package_dir.path(),
        env.destination().plan_load(different_token_input).unwrap(),
    );
    assert!(duplicate_with_different_token.duplicate);
    assert_eq!(
        duplicate_with_different_token.receipt.receipt_id,
        outcome.receipt.receipt_id
    );
}

#[test]
fn live_replace_is_atomic_and_reports_deleted_rows() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let first_dir = tempfile::tempdir().unwrap();
    let first_manifest = build_package(
        first_dir.path(),
        "pkg-live-replace-first",
        vec![("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))]))],
    );
    let first_plan = plan(
        &env,
        "orders_replace",
        &first_manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
    );
    commit(&env, first_dir.path(), first_plan);

    let second_dir = tempfile::tempdir().unwrap();
    let second_manifest = build_package(
        second_dir.path(),
        "pkg-live-replace-second",
        vec![("seg-000001", batch(&[(3, Some("katherine"))]))],
    );
    let replace_plan = plan(
        &env,
        "orders_replace",
        &second_manifest,
        WriteDisposition::Replace,
        MergeDedupPolicy::Last,
        None,
    );
    let outcome = commit(&env, second_dir.path(), replace_plan);
    assert_eq!(outcome.receipt.counts.rows_written, 1);
    assert_eq!(outcome.receipt.counts.rows_inserted, Some(1));
    assert_eq!(outcome.receipt.counts.rows_deleted, Some(2));

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"name\", \"_firn_load\" FROM {} ORDER BY \"id\"",
                env.target("orders_replace").sql()
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 3);
    assert_eq!(rows[0].get::<_, String>(1), "katherine");
    assert_eq!(rows[0].get::<_, String>(2), second_manifest.package_hash);
}

#[test]
fn live_merge_deduplicates_last_row_and_updates_existing_keys() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let first_dir = tempfile::tempdir().unwrap();
    let first_manifest = build_package(
        first_dir.path(),
        "pkg-live-merge-first",
        vec![
            ("seg-000001", batch(&[(1, Some("old")), (2, Some("two"))])),
            ("seg-000002", batch(&[(1, Some("new"))])),
        ],
    );
    let first_plan = plan(
        &env,
        "orders_merge",
        &first_manifest,
        WriteDisposition::Merge,
        MergeDedupPolicy::Last,
        None,
    );
    let first = commit(&env, first_dir.path(), first_plan);
    assert_eq!(first.receipt.counts.rows_written, 2);
    assert_eq!(first.receipt.counts.rows_inserted, Some(2));
    assert_eq!(first.receipt.counts.rows_updated, Some(0));

    let second_dir = tempfile::tempdir().unwrap();
    let second_manifest = build_package(
        second_dir.path(),
        "pkg-live-merge-second",
        vec![(
            "seg-000001",
            batch(&[(1, Some("updated")), (3, Some("three"))]),
        )],
    );
    let second_plan = plan(
        &env,
        "orders_merge",
        &second_manifest,
        WriteDisposition::Merge,
        MergeDedupPolicy::Last,
        None,
    );
    let second = commit(&env, second_dir.path(), second_plan);
    assert_eq!(second.receipt.counts.rows_written, 2);
    assert_eq!(second.receipt.counts.rows_inserted, Some(1));
    assert_eq!(second.receipt.counts.rows_updated, Some(1));

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"name\", \"_firn_segment\" FROM {} ORDER BY \"id\"",
                env.target("orders_merge").sql()
            ),
            &[],
        )
        .unwrap();
    let actual = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (1, "updated".to_owned(), "seg-000001".to_owned()),
            (2, "two".to_owned(), "seg-000001".to_owned()),
            (3, "three".to_owned(), "seg-000001".to_owned()),
        ]
    );
}

#[test]
fn live_decimal128_values_preserve_exact_numeric_text() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-decimal",
        vec![(
            "seg-000001",
            decimal_batch(&[(1, Some(1234_i128)), (2, Some(-5_i128)), (3, None)]),
        )],
    );
    let plan = plan_with_columns(
        &env,
        "orders_decimal",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
        decimal_columns(),
    );

    let outcome = commit(&env, package_dir.path(), plan);
    assert_eq!(outcome.receipt.counts.rows_written, 3);

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"amount\"::text FROM {} ORDER BY \"id\"",
                env.target("orders_decimal").sql()
            ),
            &[],
        )
        .unwrap();
    let actual = rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<String>>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (1, Some("12.34".to_owned())),
            (2, Some("-0.05".to_owned())),
            (3, None),
        ]
    );
}

#[cfg(unix)]
#[test]
fn live_package_receipt_append_error_does_not_mask_committed_database_receipt() {
    use std::os::unix::fs::PermissionsExt;

    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-receipt-append-failure",
        vec![("seg-000001", batch(&[(1, Some("ada"))]))],
    );
    let plan = plan(
        &env,
        "orders_receipt_append_failure",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
    );
    let destination_dir = package_dir.path().join("destination");
    let original_mode = std::fs::metadata(&destination_dir)
        .unwrap()
        .permissions()
        .mode();
    let mut readonly = std::fs::metadata(&destination_dir).unwrap().permissions();
    readonly.set_mode(0o500);
    std::fs::set_permissions(&destination_dir, readonly).unwrap();

    let outcome = env
        .destination()
        .commit_package(PostgresCommitRequest {
            package_dir: package_dir.path().to_path_buf(),
            plan,
        })
        .unwrap();

    let mut restored = std::fs::metadata(&destination_dir).unwrap().permissions();
    restored.set_mode(original_mode);
    std::fs::set_permissions(&destination_dir, restored).unwrap();

    assert!(!outcome.duplicate);
    assert!(!outcome.package_receipt_recorded);
    assert!(outcome.package_receipt_error.is_some());
    assert!(
        env.destination()
            .verify_receipt(&outcome.receipt)
            .unwrap()
            .verified
    );

    let mut client = env.client();
    let target_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                env.target("orders_receipt_append_failure").sql()
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(target_count, 1);
}

#[test]
fn live_rollback_after_stage_copy_leaves_no_target_or_mirror_partial_commit() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let mut client = env.client();
    let target = env.target("orders_rollback");
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (\"id\" BIGINT PRIMARY KEY, \"name\" TEXT); INSERT INTO {} (\"id\", \"name\") VALUES (1, 'seed')",
            target.sql(),
            target.sql()
        ))
        .unwrap();

    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-rollback",
        vec![("seg-000001", batch(&[(1, Some("duplicate"))]))],
    );
    let mut rollback_plan = plan(
        &env,
        "orders_rollback",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-rollback")),
    );
    rollback_plan.target_ddl = target_migrations(&PostgresLoadPlanInput {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        idempotency_token: IdempotencyToken::new(manifest.package_hash.clone()).unwrap(),
        target: target.clone(),
        disposition: WriteDisposition::Append,
        schema_hash: schema_hash(),
        segments: state_segments(&manifest),
        columns: columns(),
        merge_keys: vec![PostgresIdentifier::user("id").unwrap()],
        dedup: MergeDedupPolicy::Last,
        existing_table: Some(
            PostgresExistingTable::new(
                vec![
                    PostgresExistingColumn::new("id", "BIGINT", false).unwrap(),
                    PostgresExistingColumn::new("name", "TEXT", true).unwrap(),
                ],
                vec!["id"],
            )
            .unwrap(),
        ),
        resource_id: Some(ResourceId::new("orders").unwrap()),
        state_delta: Some(state_delta(&manifest, "chk-live-rollback")),
    })
    .unwrap();

    let error = env
        .destination()
        .commit_package(PostgresCommitRequest {
            package_dir: package_dir.path().to_path_buf(),
            plan: rollback_plan,
        })
        .unwrap_err();
    assert!(!error.message.is_empty());

    let target_rows: Vec<(i64, String)> = client
        .query(
            &format!(
                "SELECT \"id\", \"name\" FROM {} ORDER BY \"id\"",
                target.sql()
            ),
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(target_rows, vec![(1, "seed".to_owned())]);
    let load_count = load_mirror_count(&mut client, &target.display_name(), &manifest.package_hash);
    assert_eq!(load_count, 0);
    assert!(
        firn_package::PackageReader::open(package_dir.path())
            .unwrap()
            .receipts()
            .unwrap()
            .is_empty()
    );
}

fn load_mirror_count(client: &mut Client, target: &str, package_hash: &str) -> i64 {
    let table_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '_firn_loads')",
            &[],
        )
        .unwrap()
        .get(0);
    if !table_exists {
        return 0;
    }
    client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
                quote_identifier_unchecked(FIRN_LOADS_TABLE)
            ),
            &[&target, &package_hash],
        )
        .unwrap()
        .get(0)
}
