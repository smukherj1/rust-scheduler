use anyhow::{Context, Result};
use scheduler::workers_client::WorkersClient;
use tonic::transport::Channel;

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

#[derive(Debug)]
struct Runner {
    proto: scheduler::Worker,
}

impl Runner {
    async fn new(client: &mut WorkersClient<Channel>) -> Result<Self> {
        let w = scheduler::Worker {
            id: 0,
            resources: vec![
                scheduler::Resource {
                    key: "os".to_string(),
                    value: "linux".to_string(),
                },
                scheduler::Resource {
                    key: "arch".to_string(),
                    value: "amd64".to_string(),
                },
            ],
        };
        let reg = client
            .register_worker(scheduler::RegisterWorkerRequest {
                worker: Some(w.clone()),
            })
            .await
            .with_context(|| "failed to register worker")?
            .into_inner();
        println!("Registered worker {}: {:?}", reg.worker_id, w);
        Ok(Runner { proto: w })
    }

    fn id(&self) -> u64 {
        self.proto.id
    }

    async fn run_build(
        &self,
        client: &mut WorkersClient<Channel>,
        b: scheduler::AcceptBuildResponse,
    ) {
        if b.build.is_none() {
            println!("Runner {} got build with missing payload.", self.id());
            return;
        }
        let b = b.build.unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        let jh = tokio::spawn(build_heartbeats(self.id(), client.clone(), b.clone(), rx));
        sleep_seconds(b.sleep_ms / 1000).await;
        if let Err(err) = tx.send(()).await {
            println!("Runner {} unable to signal completion of build {} to the heartbeat sender background task: {err}", self.id(), b.id);
        }
        let ah = jh.abort_handle();
        if tokio::time::timeout(tokio::time::Duration::from_secs(30), jh)
            .await
            .is_err()
        {
            println!(
                "Runner {} timed out waiting for background heartbeat sender for build {} to complete, aborting the background task.",
                self.id(),
                b.id
            );
            ah.abort();
        }
    }

    async fn run(&self, client: &mut WorkersClient<Channel>) {
        println!("Starting runner {}.", self.proto.id);
        let mut idle_iterations: u64 = 0;
        loop {
            if idle_iterations > 10 && idle_iterations % 10 == 0 {
                println!(
                    "Runner {} idle for {} iterations.",
                    self.proto.id, idle_iterations
                );
            }
            let should_sleep;
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(30),
                client.accept_build(scheduler::AcceptBuildRequest {
                    worker_id: self.id(),
                }),
            )
            .await
            {
                Err(_) => should_sleep = true, // Timed out waiting for build.
                Ok(result) => {
                    match result {
                        Err(err) => {
                            should_sleep = true;
                            println!(
                                "Runner {} encountered error accepting build: {err}",
                                self.id()
                            );
                        }
                        Ok(resp) => {
                            idle_iterations = 0;
                            should_sleep = false;
                            self.run_build(client, resp.into_inner()).await;
                        }
                    };
                }
            };
            if should_sleep {
                idle_iterations += 1;
                sleep_seconds(10).await;
            }
        }
    }
}

async fn send_heartbeat(
    runner_id: u64,
    client: &mut WorkersClient<Channel>,
    build_id: u64,
    done: bool,
) {
    if let Err(err) = client
        .build_heart_beat(scheduler::BuildHeartBeatRequest {
            build_id,
            worker_id: runner_id,
            done,
        })
        .await
    {
        println!("Runner {runner_id} failed to send heartbeat with done={done} for build {build_id}: {err}");
    }
}

async fn build_heartbeats(
    runner_id: u64,
    mut client: WorkersClient<Channel>,
    b: scheduler::Build,
    mut rx: tokio::sync::mpsc::Receiver<()>,
) {
    loop {
        if tokio::time::timeout(tokio::time::Duration::from_secs(5), rx.recv())
            .await
            .is_err()
        {
            send_heartbeat(runner_id, &mut client, b.id, false).await;
            continue;
        }
        // Received build completion signal on rx.
        break;
    }
    send_heartbeat(runner_id, &mut client, b.id, true).await;
}

async fn sleep_seconds(secs: u64) {
    tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await
}

async fn launch_new_runner() -> Result<()> {
    let mut client = WorkersClient::connect("http://localhost:50051")
        .await
        .with_context(|| "failed to connect to Workers service")?;
    let r = Runner::new(&mut client)
        .await
        .with_context(|| "failed to initialize runner")?;
    r.run(&mut client).await;
    println!("Runner {} is shutting down.", r.proto.id);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let workers = 1;
    let mut js = tokio::task::JoinSet::new();
    for _ in 0..workers {
        js.spawn(launch_new_runner());
    }
    while let Some(r) = js.join_next().await {
        r.with_context(|| "runner crashed")?
            .with_context(|| "runner exited with error")?;
    }

    Ok(())
}
