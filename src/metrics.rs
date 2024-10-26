use anyhow::Result;
use once_cell::sync::Lazy;
use prometheus::{
    self, register_histogram, register_histogram_vec, register_int_counter,
    register_int_counter_vec, register_int_gauge_vec, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGaugeVec,
};
use tonic;

pub static RPC_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "scheduler_rpc_count",
        "Results of RPC calls to the scheduler by method and returned status",
        &["service", "method", "result"]
    )
    .unwrap()
});

pub static RPC_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "scheduler_rpc_latency",
        "Latency (ms) distribution of RPC calls to the scheduler by method and returned status",
        &["service", "method", "result"],
        latency_buckets(20).unwrap()
    )
    .unwrap()
});

pub static BUILDS_COMPLETED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "scheduler_builds_completed_count",
        "Count of builds completed by status",
        &["result"]
    )
    .unwrap()
});

pub static BUILD_QUEUE_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "scheduler_build_queue_latency",
        "Latency (ms) distribution of time builds were queued by completion status",
        &["result"],
        latency_buckets(20).unwrap(),
    )
    .unwrap()
});

pub static ACTIVE_BUILDS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "scheduler_active_builds",
        "Number of currently active builds broken down by total (in memory), queued and running",
        &["category"]
    )
    .unwrap()
});

pub static ACTIVE_WORKERS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "scheduler_active_workers",
        "Number of currently active workers broken down by total (in memory), idle and busy",
        &["category"]
    )
    .unwrap()
});

pub static LOCK_WAIT_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "scheduler_lock_wait_latency",
        "Latency (ms) distribution to acquire scheduler locks by type",
        &["lock"],
        latency_buckets(20).unwrap(),
    )
    .unwrap()
});

pub static LOCK_WAITERS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "scheduler_lock_waiters",
        "Numer of concurrent threads waiting to acquire a scheduler lock by lock name",
        &["lock"],
    )
    .unwrap()
});

pub static GC_CYCLES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "scheduler_gc_cycles",
        "Number of scheduler garbage collection cycles"
    )
    .unwrap()
});

pub static GC_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "scheduler_gc_latency",
        "Latency (ms) distribution of garbage collection cycles",
        latency_buckets(20).unwrap(),
    )
    .unwrap()
});

fn latency_buckets(count: usize) -> Result<Vec<f64>> {
    if count > 30 {
        anyhow::bail!("bucket count {count} for latency_buckets is too high, must be <= 30");
    }
    let mut result = Vec::with_capacity(count + 1);
    result.push(0f64);
    let mut next = 1f64;
    for _ in 0..count {
        result.push(next);
        next *= 2f64;
    }
    Ok(result)
}

pub fn code_str(c: tonic::Code) -> &'static str {
    match c {
        tonic::Code::Ok => "ok",
        tonic::Code::Internal => "internal",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::Aborted => "aborted",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::NotFound => "not_found",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::Unauthenticated => "unauthenticated",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Unknown => "unknown",
    }
}
