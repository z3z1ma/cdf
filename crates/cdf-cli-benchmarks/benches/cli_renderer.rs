use std::collections::BTreeMap;

use cdf_cli_core::{
    progress::{CliProgressSink, DisplayVerbosity, ProgressConfig},
    render::{
        RenderConfig, RenderDocument,
        primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
    },
};
use cdf_kernel::{
    PackageHash, PartitionId, ResourceId, RunEvent, RunEventDetails, RunEventKind, RunEventSink,
    RunId, ScopeKey,
};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

const EVENT_COUNT: u64 = 1_000_000;
const HIGH_PARTITION_COUNT: u64 = 10_000;
const LARGE_REPORT_ROW_COUNT: u64 = 10_000;

fn progress_event(sequence: u64) -> RunEvent {
    RunEvent {
        run_id: RunId::new("run-cli-renderer-benchmark").expect("valid run id"),
        sequence,
        timestamp_ms: i64::try_from(sequence).expect("benchmark sequence fits i64"),
        kind: RunEventKind::PackageSegmentRecorded,
        resource_id: Some(ResourceId::new("benchmark.events").expect("valid resource id")),
        scope: None,
        partition_id: None,
        package_id: Some("pkg-cli-renderer-benchmark".to_owned()),
        package_hash: Some(
            PackageHash::new("sha256:cli-renderer-benchmark").expect("valid package hash"),
        ),
        package_path: None,
        checkpoint_id: None,
        receipt_id: None,
        destination_id: None,
        plan_id: None,
        details: RunEventDetails {
            attributes: BTreeMap::new(),
        },
    }
}

fn large_report_document() -> RenderDocument {
    let mut resources = Table::new(["resource", "state", "rows", "bytes", "receipt"]);
    for index in 1..=LARGE_REPORT_ROW_COUNT {
        resources = resources.row([
            format!("resource-{index:08}"),
            "complete".to_owned(),
            (index * 100).to_string(),
            (index * 4_096).to_string(),
            format!("receipt-{index:08}"),
        ]);
    }

    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            "large report rendered",
        ))
        .blank_line()
        .push(
            KeyValuePanel::summary()
                .row("resources", LARGE_REPORT_ROW_COUNT.to_string())
                .row("state", "complete"),
        )
        .blank_line()
        .push(resources)
        .blank_line()
        .push(NextCommand::new("cdf inspect run run-large-report"))
}

fn cli_renderer(c: &mut Criterion) {
    let mut group = c.benchmark_group("cli_renderer");
    group.throughput(Throughput::Elements(EVENT_COUNT));
    group.bench_function("million_event_iteration_baseline", |bench| {
        bench.iter(|| {
            let mut event = progress_event(1);
            for sequence in 1..=EVENT_COUNT {
                event.sequence = sequence;
                event.timestamp_ms = i64::try_from(sequence).expect("benchmark sequence fits i64");
                black_box(&event);
            }
        });
    });
    group.bench_function("million_buffered_events", |bench| {
        bench.iter(|| {
            let sink = CliProgressSink::new(ProgressConfig::new(
                RenderConfig::headless_for_width(80),
                DisplayVerbosity::Normal,
            ));
            let mut event = progress_event(1);
            for sequence in 1..=EVENT_COUNT {
                event.sequence = sequence;
                event.timestamp_ms = i64::try_from(sequence).expect("benchmark sequence fits i64");
                let _ = sink.try_emit(&event);
            }
            sink.finish()
        });
    });
    group.finish();

    let high_partition_events = (1..=HIGH_PARTITION_COUNT)
        .map(|sequence| {
            let partition_id =
                PartitionId::new(format!("partition-{sequence:08}")).expect("valid partition id");
            let mut event = progress_event(sequence);
            event.partition_id = Some(partition_id.clone());
            event.scope = Some(ScopeKey::Partition { partition_id });
            event
        })
        .collect::<Vec<_>>();
    let mut group = c.benchmark_group("cli_renderer_high_partition");
    group.throughput(Throughput::Elements(HIGH_PARTITION_COUNT));
    group.bench_function("bounded_buffered_events", |bench| {
        bench.iter(|| {
            let sink = CliProgressSink::new(ProgressConfig::new(
                RenderConfig::headless_for_width(80),
                DisplayVerbosity::Normal,
            ));
            for event in &high_partition_events {
                let _ = sink.try_emit(event);
            }
            sink.finish()
        });
    });
    group.finish();

    let large_report = large_report_document();
    let large_report_config = RenderConfig::headless_for_width(160);
    let large_report_output = large_report.render(&large_report_config);
    assert_eq!(
        large_report_output.matches("resource-").count(),
        usize::try_from(LARGE_REPORT_ROW_COUNT).expect("benchmark row count fits usize"),
        "large report benchmark fixture must render every row"
    );
    assert!(large_report_output.contains("resource-00000001"));
    assert!(large_report_output.contains("resource-00010000"));

    let mut group = c.benchmark_group("cli_renderer_large_report");
    group.throughput(Throughput::Elements(LARGE_REPORT_ROW_COUNT));
    group.bench_function("ten_thousand_row_prebuilt_headless_report", |bench| {
        bench.iter(|| black_box(large_report.render(&large_report_config)));
    });
    group.bench_function("ten_thousand_row_build_and_render", |bench| {
        bench.iter(|| {
            let document = large_report_document();
            black_box(document.render(&large_report_config))
        });
    });
    group.finish();
}

criterion_group!(benches, cli_renderer);
criterion_main!(benches);
