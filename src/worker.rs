use anyhow::{Context, Result};
use scheduler::{
    workers_client::WorkersClient, AcceptBuildRequest, AcceptBuildResponse, BuildHeartBeatRequest,
    BuildHeartBeatResponse, RegisterWorkerRequest, RegisterWorkerResponse, Resource, Worker,
};
use tonic::transport::Channel;

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

#[derive(Debug)]
struct Runner {
    proto: Worker,
    client: WorkersClient<Channel>,
}

impl Runner {
    async fn new() -> Result<Self> {
        let mut client = WorkersClient::connect("http://localhost:50051")
            .await
            .with_context(|| "failed to connect to Workers service")?;

        let w = Worker {
            id: 0,
            resources: vec![
                Resource {
                    key: "os".to_string(),
                    value: "linux".to_string(),
                },
                Resource {
                    key: "arch".to_string(),
                    value: "amd64".to_string(),
                },
            ],
        };
        let reg = client
            .register_worker(RegisterWorkerRequest {
                worker: Some(w.clone()),
            })
            .await
            .with_context(|| "failed to register worker")?
            .into_inner();
        println!("Registered worker {}: {:?}", reg.worker_id, w);
        Ok(Runner { proto: w, client })
    }

    async fn run(&mut self) -> Result<u64> {
        println!("Starting runner {}.", self.proto.id);
        Ok(self.proto.id)
    }
}

async fn run() -> Result<()> {
    let mut r = Runner::new()
        .await
        .with_context(|| "failed to initialize runner")?;
    r.run()
        .await
        .with_context(|| format!("runner {} quit with error", r.proto.id))?;
    println!("Runner {} is shutting down.", r.proto.id);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let workers = 1;
    let mut js = tokio::task::JoinSet::new();
    for _ in 0..workers {
        js.spawn(run());
    }
    while let Some(r) = js.join_next().await {
        r.with_context(|| "runner crashed")?
            .with_context(|| "runner exited with error")?;
    }

    Ok(())
}
