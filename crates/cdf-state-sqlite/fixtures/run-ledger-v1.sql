PRAGMA foreign_keys = ON;

CREATE TABLE cdf_sqlite_schema_migrations (
    component TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    applied_at_ms INTEGER NOT NULL
);

INSERT INTO cdf_sqlite_schema_migrations (component, version, applied_at_ms)
VALUES ('run_ledger', 1, 1700000000000);

CREATE TABLE cdf_runs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE cdf_run_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES cdf_runs(run_id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    timestamp_ms INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
        'run_started',
        'plan_recorded',
        'package_started',
        'package_finalized',
        'destination_commit_started',
        'destination_receipt_recorded',
        'checkpoint_proposed',
        'checkpoint_committed',
        'package_status_updated',
        'run_succeeded',
        'run_failed',
        'run_resumed',
        'replay_recorded'
    )),
    resource_id TEXT,
    scope_json TEXT,
    partition_id TEXT,
    package_id TEXT,
    package_hash TEXT,
    package_path TEXT,
    checkpoint_id TEXT,
    receipt_id TEXT,
    destination_id TEXT,
    plan_id TEXT,
    details_json TEXT NOT NULL,
    UNIQUE(run_id, sequence)
);

CREATE INDEX cdf_run_events_run_sequence
    ON cdf_run_events (run_id, sequence);

INSERT INTO cdf_runs (run_id, created_at_ms)
VALUES ('run-v1-fixture', 1700000000000);

INSERT INTO cdf_run_events (
    run_id,
    sequence,
    timestamp_ms,
    kind,
    details_json
)
VALUES (
    'run-v1-fixture',
    1,
    1700000000001,
    'run_started',
    '{"attributes":{}}'
);
