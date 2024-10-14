mod queue;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use once_cell::sync::Lazy;
use prometheus::{self, register_int_counter_vec, Encoder, IntCounterVec, TextEncoder};
use queue::scheduler;
use scheduler::builds_server::{Builds, BuildsServer};
use scheduler::workers_server::{Workers, WorkersServer};
use scheduler::{
    AcceptBuildRequest, AcceptBuildResponse, BuildHeartBeatRequest, BuildHeartBeatResponse,
    CreateBuildRequest, CreateBuildResponse, RegisterWorkerRequest, RegisterWorkerResponse,
    WaitBuildRequest, WaitBuildResponse,
};
use tonic::{transport::Server, Request, Response, Status};

static BUILDS_STARTED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "builds_started_count",
        "Number of builds started",
        &["status"]
    )
    .unwrap()
});

static BUILDS_COMPLETED_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "builds_completed_count",
        "Number of completed builds",
        &["status"]
    )
    .unwrap()
});

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

pub struct BuildsService<'a> {
    q: &'a queue::Queue,
}

impl<'a> BuildsService<'a> {
    fn new(q: &'a queue::Queue) -> Self {
        BuildsService { q }
    }
}

#[tonic::async_trait]
impl Builds for BuildsService<'static> {
    async fn create_build(
        &self,
        request: Request<CreateBuildRequest>,
    ) -> Result<Response<CreateBuildResponse>, Status> {
        let report_metric = |c: tonic::Code| {
            BUILDS_STARTED_COUNT
                .with_label_values(&[format!("{c}").as_str()])
                .inc();
        };
        let b = request
            .into_inner()
            .build
            .ok_or(tonic::Status::invalid_argument(
                "field build can't be missing in create_build",
            ))
            .inspect_err(|st| report_metric(st.code()))?;
        let build_id = self
            .q
            .create_build(b)
            .inspect_err(|st| report_metric(st.code()))?;
        let resp = CreateBuildResponse { build_id };

        BUILDS_STARTED_COUNT.with_label_values(&["ok"]).inc();
        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn wait_build(
        &self,
        request: Request<WaitBuildRequest>,
    ) -> Result<Response<WaitBuildResponse>, Status> {
        let req = request.into_inner();
        let resp = self
            .q
            .wait_build(req.build_id)
            .map_err(|err| Status::new(err.code(), format!("error waiting for build: {err:?}")))?;
        let build_result = match resp {
            queue::BuildResultResponse::BuildResult(br) => br,
            queue::BuildResultResponse::WaitChannel(rx) => rx.await.map_err(|err| {
                Status::internal(format!(
                    "error receiving build from scheduler queue: {err:?}"
                ))
            })?,
        };
        BUILDS_COMPLETED_COUNT
            .with_label_values(
                &[format!("{}", tonic::Code::from_i32(build_result.status)).as_str()],
            )
            .inc();
        Ok(Response::new(WaitBuildResponse {
            build_result: Some(build_result),
        }))
    }
}

pub struct WorkersService<'a> {
    q: &'a queue::Queue,
}

impl<'a> WorkersService<'a> {
    fn new(q: &'a queue::Queue) -> Self {
        WorkersService { q }
    }
}

#[tonic::async_trait]
impl Workers for WorkersService<'static> {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        let worker = req
            .worker
            .ok_or_else(|| Status::invalid_argument("missing field: worker"))?;
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
        let req = request.into_inner();
        let resp = self.q.accept_build(req.worker_id).map_err(|err| {
            Status::new(
                err.code(),
                format!("error accepting build for worker: {err:?}"),
            )
        })?;
        let build = match resp {
            queue::BuildResponse::Build(b) => b,
            queue::BuildResponse::WaitChannel(rx) => rx.await.map_err(|err| {
                Status::internal(format!(
                    "error receiving build for worker from scheduler queue: {err:?}"
                ))
            })?,
        };
        Ok(Response::new(AcceptBuildResponse { build: Some(build) }))
    }

    async fn build_heart_beat(
        &self,
        request: Request<BuildHeartBeatRequest>,
    ) -> Result<Response<BuildHeartBeatResponse>, Status> {
        let req = request.into_inner();
        self.q
            .build_heartbeat(req.worker_id, req.build_id, req.done)
            .map_err(|err| {
                Status::new(
                    err.code(),
                    format!("error accepting heartbeat for build: {err:?}"),
                )
            })?;
        Ok(Response::new(BuildHeartBeatResponse {}))
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

async fn launch_scheduler_server(args: Arc<Args>) -> Result<()> {
    let addr = args.address.parse().with_context(|| {
        format!(
            "failed to parse {} as a socker address for scheduler GRPC server",
            args.address
        )
    })?;
    static Q: Lazy<queue::Queue> = Lazy::new(queue::Queue::new);
    let bs = BuildsService::new(&Q);
    let ws = WorkersService::new(&Q);
    println!("Launching scheduler GRPC server on {:?}", addr);
    Server::builder()
        .add_service(BuildsServer::new(bs))
        .add_service(WorkersServer::new(ws))
        .serve(addr)
        .await
        .with_context(|| format!("failed to launch GRPC server on {:?}", addr))?;
    Ok(())
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

    let margs = Arc::clone(&args);
    js.spawn(launch_metrics_server(margs));
    js.spawn(launch_scheduler_server(args));

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
