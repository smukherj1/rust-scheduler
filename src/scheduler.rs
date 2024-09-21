use tonic::{transport::Server, Request, Response, Status};

use scheduler::builds_server::{Builds, BuildsServer};
use scheduler::workers_server::{Workers, WorkersServer};
use scheduler::{
    AcceptBuildRequest, AcceptBuildResponse, BuildHeartBeatRequest, BuildHeartBeatResponse,
    RegisterWorkerRequest, RegisterWorkerResponse,
};
use scheduler::{CreateBuildRequest, CreateBuildResponse};

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

#[derive(Debug, Default)]
pub struct BuildsService {}

#[tonic::async_trait]
impl Builds for BuildsService {
    async fn create_build(
        &self,
        request: Request<CreateBuildRequest>,
    ) -> Result<Response<CreateBuildResponse>, Status> {
        println!("Got a request: {:?}", request);

        let resp = CreateBuildResponse { build_id: 1 };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }
}

#[derive(Debug, Default)]
pub struct WorkersService {}

#[tonic::async_trait]
impl Workers for WorkersService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        println!("Got a request: {:?}", request);

        let resp = RegisterWorkerResponse { worker_id: 1 };

        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn accept_build(
        &self,
        request: Request<AcceptBuildRequest>,
    ) -> Result<Response<AcceptBuildResponse>, Status> {
        println!("Got a request: {:?}", request);

        let resp = AcceptBuildResponse::default();

        Ok(Response::new(resp)) // Send back our formatted greeting
    }

    async fn build_heart_beat(
        &self,
        request: Request<BuildHeartBeatRequest>,
    ) -> Result<Response<BuildHeartBeatResponse>, Status> {
        println!("Got a request: {:?}", request);

        let resp = BuildHeartBeatResponse::default();

        Ok(Response::new(resp)) // Send back our formatted greeting
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let bs = BuildsService::default();
    let ws = WorkersService::default();
    println!("Launching server on {:?}", addr);
    Server::builder()
        .add_service(BuildsServer::new(bs))
        .add_service(WorkersServer::new(ws))
        .serve(addr)
        .await?;

    Ok(())
}
