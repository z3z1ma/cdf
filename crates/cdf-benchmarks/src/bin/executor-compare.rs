use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use cdf_engine::StandaloneExecutionHost;
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_runtime::{CpuTaskSpec, ExecutionHost, ExecutionHostCapabilities};
use sha2::{Digest, Sha256};

const TASKS_PER_CORE: usize = 64;
const HASH_ROUNDS: usize = 2_048;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cores = std::thread::available_parallelism()?
        .get()
        .min(usize::from(u16::MAX));
    let tasks = cores * TASKS_PER_CORE;
    let standalone = standalone(cores, tasks)?;
    let standalone_future = standalone_future(cores, tasks)?;
    let tokio = tokio_blocking(cores, tasks)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 2,
            "logical_cpus": cores,
            "tasks": tasks,
            "hash_rounds_per_task": HASH_ROUNDS,
            "fixed_cpu_pool_elapsed_ns": standalone,
            "fixed_cpu_future_elapsed_ns": standalone_future,
            "tokio_spawn_blocking_elapsed_ns": tokio,
            "fixed_over_tokio_ratio": standalone as f64 / tokio as f64,
            "fixed_future_over_fixed_ratio": standalone_future as f64 / standalone as f64,
        }))?
    );
    Ok(())
}

fn standalone_future(cores: usize, tasks: usize) -> Result<u128, Box<dyn std::error::Error>> {
    let memory: Arc<dyn MemoryCoordinator> = Arc::new(DeterministicMemoryCoordinator::new(
        1024 * 1024,
        BTreeMap::new(),
    )?);
    let host = StandaloneExecutionHost::new(
        ExecutionHostCapabilities {
            logical_cpu_slots: u16::try_from(cores)?,
            io_workers: 1,
            blocking_lanes: Vec::new(),
        },
        memory,
    )?;
    let checksum = Arc::new(AtomicU64::new(0));
    let mut scope = host.open_scope("executor-future-compare")?;
    let started = Instant::now();
    for task_id in 0..tasks {
        let checksum = Arc::clone(&checksum);
        scope.spawn_cpu_future(
            CpuTaskSpec {
                task_kind: "sha256.future".to_owned(),
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
            },
            Box::pin(async move {
                tokio::task::yield_now().await;
                checksum.fetch_xor(hash_work(task_id), Ordering::Relaxed);
                Ok(())
            }),
        )?;
    }
    futures_executor::block_on(scope.join())?;
    std::hint::black_box(checksum.load(Ordering::Relaxed));
    Ok(started.elapsed().as_nanos())
}

fn standalone(cores: usize, tasks: usize) -> Result<u128, Box<dyn std::error::Error>> {
    let memory: Arc<dyn MemoryCoordinator> = Arc::new(DeterministicMemoryCoordinator::new(
        1024 * 1024,
        BTreeMap::new(),
    )?);
    let host = StandaloneExecutionHost::new(
        ExecutionHostCapabilities {
            logical_cpu_slots: u16::try_from(cores)?,
            io_workers: 1,
            blocking_lanes: Vec::new(),
        },
        memory,
    )?;
    let checksum = Arc::new(AtomicU64::new(0));
    let mut scope = host.open_scope("executor-compare")?;
    let started = Instant::now();
    for task_id in 0..tasks {
        let checksum = Arc::clone(&checksum);
        scope.spawn_cpu(
            CpuTaskSpec {
                task_kind: "sha256".to_owned(),
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
            },
            Box::new(move || {
                checksum.fetch_xor(hash_work(task_id), Ordering::Relaxed);
                Ok(())
            }),
        )?;
    }
    futures_executor::block_on(scope.join())?;
    std::hint::black_box(checksum.load(Ordering::Relaxed));
    Ok(started.elapsed().as_nanos())
}

fn tokio_blocking(cores: usize, tasks: usize) -> Result<u128, Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(cores)
        .build()?;
    let checksum = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    runtime.block_on(async {
        let mut handles = Vec::with_capacity(tasks);
        for task_id in 0..tasks {
            let checksum = Arc::clone(&checksum);
            handles.push(tokio::task::spawn_blocking(move || {
                checksum.fetch_xor(hash_work(task_id), Ordering::Relaxed);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    });
    std::hint::black_box(checksum.load(Ordering::Relaxed));
    Ok(started.elapsed().as_nanos())
}

fn hash_work(task_id: usize) -> u64 {
    let mut value = task_id.to_le_bytes().to_vec();
    for _ in 0..HASH_ROUNDS {
        value = Sha256::digest(&value).to_vec();
    }
    u64::from_le_bytes(value[..8].try_into().unwrap())
}
