use std::sync::atomic;

use tonic::{transport::Server, Request, Response, Status};

use once_cell::sync::Lazy;
use scheduler::builds_server::{Builds, BuildsServer};
use scheduler::workers_server::{Workers, WorkersServer};
use scheduler::{
    AcceptBuildRequest, AcceptBuildResponse, BuildHeartBeatRequest, BuildHeartBeatResponse,
    RegisterWorkerRequest, RegisterWorkerResponse,
};
use scheduler::{CreateBuildRequest, CreateBuildResponse};
mod queue;

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

#[derive(Debug)]
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
        println!("Builds.create_build: {:?}", request.into_inner());
        let resp = CreateBuildResponse { build_id: 1 };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }
}

#[derive(Debug)]
pub struct WorkersService<'a> {
    q: &'a queue::Queue,
    next_id: atomic::AtomicU64,
}

impl<'a> WorkersService<'a> {
    fn new(q: &'a queue::Queue) -> Self {
        WorkersService {
            q,
            next_id: 0.into(),
        }
    }
}

#[tonic::async_trait]
impl Workers for WorkersService<'static> {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        println!("Workers.register_worker: {:?}", req);
        let worker_id = self.next_id.fetch_add(1, atomic::Ordering::Relaxed);

        let resp = RegisterWorkerResponse { worker_id };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn accept_build(
        &self,
        request: Request<AcceptBuildRequest>,
    ) -> Result<Response<AcceptBuildResponse>, Status> {
        let req = request.into_inner();
        println!("Workers.accept_build: {:?}", req);
        let wid = req.worker_id;
        if wid >= self.next_id.load(atomic::Ordering::Relaxed) {
            return Err(Status::invalid_argument(format!(
                "worker {wid} has not been registered"
            )));
        }

        let resp = AcceptBuildResponse {
            build: Some(scheduler::Build {
                id: 1,
                requirements: vec![],
                sleep_ms: 10000,
            }),
        };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn build_heart_beat(
        &self,
        request: Request<BuildHeartBeatRequest>,
    ) -> Result<Response<BuildHeartBeatResponse>, Status> {
        let req = request.into_inner();
        println!("Workers.build_heart_beat: {:?}", req);
        let wid = req.worker_id;
        if wid >= self.next_id.load(atomic::Ordering::Relaxed) {
            return Err(Status::invalid_argument(format!(
                "worker {wid} has not been registered"
            )));
        }

        let resp = BuildHeartBeatResponse::default();
        Ok(Response::new(resp)) // Send back our formatted greeting
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
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
