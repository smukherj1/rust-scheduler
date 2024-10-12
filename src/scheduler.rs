mod queue;

use anyhow::{Context, Result};
use clap::Parser;
use once_cell::sync::Lazy;
use queue::scheduler;
use scheduler::builds_server::{Builds, BuildsServer};
use scheduler::workers_server::{Workers, WorkersServer};
use scheduler::{
    AcceptBuildRequest, AcceptBuildResponse, BuildHeartBeatRequest, BuildHeartBeatResponse,
    RegisterWorkerRequest, RegisterWorkerResponse, WaitBuildRequest, WaitBuildResponse,
};
use scheduler::{CreateBuildRequest, CreateBuildResponse};
use tonic::{transport::Server, Request, Response, Status};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// URL for the address this scheduler GRPC server will serve at.
    #[arg(short, long)]
    address: String,
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
        let b = request
            .into_inner()
            .build
            .ok_or(tonic::Status::invalid_argument(
                "field build can't be missing in create_build",
            ))?;
        let build_id = self.q.create_build(b)?;
        let resp = CreateBuildResponse { build_id };

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
        let build = match resp {
            queue::BuildResponse::Build(b) => b,
            queue::BuildResponse::WaitChannel(rx) => rx.await.map_err(|err| {
                Status::internal(format!(
                    "error receiving build from scheduler queue: {err:?}"
                ))
            })?,
        };
        Ok(Response::new(WaitBuildResponse { build: Some(build) }))
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

fn validate_args(args: &Args) -> Result<()> {
    if args.address.is_empty() {
        anyhow::bail!("address flag can't be empty");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args).with_context(|| "failed to validate command line arguments")?;
    let addr = args
        .address
        .parse()
        .with_context(|| format!("failed to parse {} as a socker address", args.address))?;
    static Q: Lazy<queue::Queue> = Lazy::new(queue::Queue::new);
    let bs = BuildsService::new(&Q);
    let ws = WorkersService::new(&Q);
    println!("Launching server on {:?}", addr);
    Server::builder()
        .add_service(BuildsServer::new(bs))
        .add_service(WorkersServer::new(ws))
        .serve(addr)
        .await?;

    Ok(())
}
