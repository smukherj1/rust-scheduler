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
        println!("Builds.create_build: {:?}", request);

        let resp = CreateBuildResponse { build_id: 1 };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }
}

#[derive(Debug)]
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
        println!("Workers.register_worker: {:?}", request);

        let resp = RegisterWorkerResponse { worker_id: 1 };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn accept_build(
        &self,
        request: Request<AcceptBuildRequest>,
    ) -> Result<Response<AcceptBuildResponse>, Status> {
        println!("Workers.accept_build: {:?}", request);

        let resp = AcceptBuildResponse::default();

        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn build_heart_beat(
        &self,
        request: Request<BuildHeartBeatRequest>,
    ) -> Result<Response<BuildHeartBeatResponse>, Status> {
        println!("Workers.build_heart_beat: {:?}", request);

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
