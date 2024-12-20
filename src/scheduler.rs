mod metrics;
mod queue;
use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use prometheus::{self, Encoder, TextEncoder};
use queue::scheduler;
use scheduler::builds_server::{Builds, BuildsServer};
use scheduler::workers_server::{Workers, WorkersServer};
use scheduler::{
    AcceptBuildRequest, AcceptBuildResponse, BuildHeartBeatRequest, BuildHeartBeatResponse,
    CreateBuildRequest, CreateBuildResponse, RegisterWorkerRequest, RegisterWorkerResponse,
    WaitBuildRequest, WaitBuildResponse,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::{transport::Server, Request, Response, Status};

struct RpcReporter<'a> {
    service: &'a str,
    method: &'a str,
    status: tonic::Status,
    begin: std::time::Instant,
}

impl<'a> RpcReporter<'a> {
    fn new(service: &'a str, method: &'a str) -> Self {
        RpcReporter {
            service,
            method,
            status: tonic::Status::ok(""),
            begin: std::time::Instant::now(),
        }
    }

    fn update_status(&mut self, st: &tonic::Status) {
        self.status = st.clone();
    }
}

impl<'a> Drop for RpcReporter<'a> {
    fn drop(&mut self) {
        let code = metrics::code_str(self.status.code());
        metrics::RPC_COUNT
            .with_label_values(&[self.service, self.method, code])
            .inc();
        metrics::RPC_LATENCY
            .with_label_values(&[self.service, self.method, code])
            .observe(
                std::time::Instant::now()
                    .checked_duration_since(self.begin)
                    .unwrap_or(Duration::from_millis(0))
                    .as_millis() as f64,
            );
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// URL for the address this scheduler GRPC server will serve at.
    #[arg(short, long)]
    address: String,

    /// URL for the address to serve metrics at.
    #[arg(short, long)]
    metrics_address: String,
}

pub struct BuildsService {
    q: Arc<queue::Queue>,
}

impl BuildsService {
    fn new(q: Arc<queue::Queue>) -> Self {
        BuildsService { q }
    }
}

#[tonic::async_trait]
impl Builds for BuildsService {
    async fn create_build(
        &self,
        request: Request<CreateBuildRequest>,
    ) -> Result<Response<CreateBuildResponse>, Status> {
        let mut r = RpcReporter::new("builds", "create_build");
        let b = request
            .into_inner()
            .build
            .ok_or(tonic::Status::invalid_argument(
                "field build can't be missing in create_build",
            ))
            .inspect_err(|st| r.update_status(st))?;
        let build_id = self
            .q
            .create_build(b)
            .inspect_err(|st| r.update_status(st))?;
        let resp = CreateBuildResponse { build_id };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn wait_build(
        &self,
        request: Request<WaitBuildRequest>,
    ) -> Result<Response<WaitBuildResponse>, Status> {
        let mut r = RpcReporter::new("builds", "wait_build");
        let req = request.into_inner();
        let resp = self
            .q
            .wait_build(req.build_id)
            .map_err(|err| Status::new(err.code(), format!("error waiting for build: {err:?}")))
            .inspect_err(|st| r.update_status(st))?;
        let build_result = match resp {
            queue::BuildResultResponse::BuildResult(br) => br,
            queue::BuildResultResponse::WaitChannel(rx) => rx
                .await
                .map_err(|err| {
                    Status::internal(format!(
                        "error receiving build from scheduler queue: {err:?}"
                    ))
                })
                .inspect_err(|st| r.update_status(st))?,
        };
        Ok(Response::new(WaitBuildResponse {
            build_result: Some(build_result),
        }))
    }
}

pub struct WorkersService {
    q: Arc<queue::Queue>,
}

impl WorkersService {
    fn new(q: Arc<queue::Queue>) -> Self {
        WorkersService { q }
    }
}

#[tonic::async_trait]
impl Workers for WorkersService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let mut r = RpcReporter::new("workers", "register_worker");
        let req = request.into_inner();
        let worker = req
            .worker
            .ok_or_else(|| Status::invalid_argument("missing field: worker"))
            .inspect_err(|st| r.update_status(st))?;
        let worker_id = self.q.register_worker(&worker.resources).map_err(|err| {
            Status::new(err.code(), format!("unable to register worker: {err:?}"))
        })?;

        let resp = RegisterWorkerResponse { worker_id };

        Ok(Response::new(resp))
    }

    async fn accept_build(
        &self,
        request: Request<AcceptBuildRequest>,
    ) -> Result<Response<AcceptBuildResponse>, Status> {
        let mut r = RpcReporter::new("workers", "accept_build");
        let req = request.into_inner();
        let resp = self
            .q
            .accept_build(req.worker_id)
            .map_err(|err| {
                Status::new(
                    err.code(),
                    format!("error accepting build for worker: {err:?}"),
                )
            })
            .inspect_err(|st| r.update_status(st))?;
        let build = match resp {
            queue::BuildResponse::Build(b) => b,
            queue::BuildResponse::WaitChannel(rx) => rx
                .await
                .map_err(|err| {
                    Status::internal(format!(
                        "error receiving build for worker from scheduler queue: {err:?}"
                    ))
                })
                .inspect_err(|st| r.update_status(st))?,
        };
        Ok(Response::new(AcceptBuildResponse { build: Some(build) }))
    }

    async fn build_heart_beat(
        &self,
        request: Request<BuildHeartBeatRequest>,
    ) -> Result<Response<BuildHeartBeatResponse>, Status> {
        let mut r = RpcReporter::new("workers", "build_heart_beat");
        let req = request.into_inner();
        self.q
            .build_heartbeat(
                req.worker_id,
                req.build_id,
                req.done,
                req.status,
                req.details,
            )
            .map_err(|err| {
                Status::new(
                    err.code(),
                    format!("error accepting heartbeat for build: {err:?}"),
                )
            })
            .inspect_err(|st| r.update_status(st))?;
        Ok(Response::new(BuildHeartBeatResponse {}))
    }
}

async fn periodically_report_queue_metrics(q: Arc<queue::Queue>) -> Result<()> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        let m = q.stats();
        metrics::ACTIVE_BUILDS
            .with_label_values(&["total"])
            .set(m.total_builds as i64);
        metrics::ACTIVE_BUILDS
            .with_label_values(&["queued"])
            .set(m.queued_builds as i64);
        metrics::ACTIVE_BUILDS
            .with_label_values(&["running"])
            .set(m.running_builds as i64);
        metrics::ACTIVE_WORKERS
            .with_label_values(&["total"])
            .set(m.total_workers as i64);
        metrics::ACTIVE_WORKERS
            .with_label_values(&["idle"])
            .set(m.idle_workers as i64);
        metrics::ACTIVE_WORKERS
            .with_label_values(&["busy"])
            .set(m.busy_workers as i64);
    }
}

async fn metrics() -> Result<String, axum::http::StatusCode> {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    encoder
        .encode(&prometheus::gather(), &mut buffer)
        .expect("Failed to encode metrics");

    let response = String::from_utf8(buffer).expect("Failed to convert bytes to string");
    Ok(response)
}

async fn launch_metrics_server(args: Arc<Args>) -> Result<()> {
    let app = Router::new().route("/metrics", axum::routing::get(metrics));
    let listener = tokio::net::TcpListener::bind(args.metrics_address.as_str())
        .await
        .with_context(|| {
            format!(
                "error initializing listener for metrics server at address {}",
                args.metrics_address.as_str()
            )
        })?;
    println!(
        "Launching scheduler metrics server on {}/metrics",
        args.metrics_address
    );
    axum::serve(listener, app).await.with_context(|| {
        format!(
            "metrics server at address {} crashed",
            args.metrics_address.as_str()
        )
    })?;
    Ok(())
}

async fn launch_scheduler_server(q: Arc<queue::Queue>, args: Arc<Args>) -> Result<()> {
    let addr = args.address.parse().with_context(|| {
        format!(
            "failed to parse {} as a socker address for scheduler GRPC server",
            args.address
        )
    })?;
    let bs = BuildsService::new(Arc::clone(&q));
    let ws = WorkersService::new(q);
    println!("Launching scheduler GRPC server on {:?}", addr);
    Server::builder()
        .add_service(BuildsServer::new(bs))
        .add_service(WorkersServer::new(ws))
        .serve(addr)
        .await
        .with_context(|| format!("failed to launch GRPC server on {:?}", addr))?;
    Ok(())
}

async fn launch_gc(q: Arc<queue::Queue>) -> Result<()> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        q.gc()
            .with_context(|| "scheduler queue garbage collection failure")?;
    }
}

fn validate_args(args: &Args) -> Result<()> {
    if args.address.is_empty() {
        anyhow::bail!("address flag can't be empty");
    }
    if args.metrics_address.is_empty() {
        anyhow::bail!("metrica_address flag can't be empty");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Arc::new(Args::parse());
    validate_args(&args).with_context(|| "failed to validate command line arguments")?;

    let mut js = tokio::task::JoinSet::new();

    let q = Arc::new(queue::Queue::new());
    let margs = Arc::clone(&args);
    js.spawn(launch_metrics_server(margs));
    js.spawn(launch_scheduler_server(Arc::clone(&q), args));
    js.spawn(launch_gc(Arc::clone(&q)));
    js.spawn(periodically_report_queue_metrics(q));

    while let Some(res) = js.join_next().await {
        let res = match res {
            Ok(r) => r,
            Err(err) => {
                eprintln!("Shutting down due to error joining spawned server threads: {err:?}");
                js.shutdown().await;
                break;
            }
        };
        if let Err(err) = res {
            eprintln!("Shutting down due to error from a spawned server thread: {err:?}");
            js.shutdown().await;
            break;
        }
    }

    Ok(())
}
