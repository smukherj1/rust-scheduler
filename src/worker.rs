use anyhow::{Context, Result};
use clap::Parser;
use rand::distributions::Uniform;
use rand::Rng;
use scheduler::workers_client::WorkersClient;
use std::sync::Arc;
use tonic::transport::Channel;

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// URL for the scheduler GRPC server.
    #[arg(short, long)]
    scheduler_addr: String,

    /// Number of worker tasks to spawn.
    #[arg(short, long, default_value_t = 1)]
    workers: i32,

    /// Probability of a build failing [0-100].
    #[arg(long, default_value_t = 0)]
    prob_build_failure: i32,

    /// Probability of a worker disappearing between
    /// build heartbeats [0-100].
    #[arg(long, default_value_t = 0)]
    prob_build_heartbeat_failure: i32,

    /// Probability of a worker disappearing on an
    /// idle iteration between accept_build calls
    /// [0-100].
    #[arg(long, default_value_t = 0)]
    prob_idle_failure: i32,
}

#[derive(Debug)]
struct Runner {
    proto: scheduler::Worker,
    args: Arc<Args>,
    prob_dist: Uniform<i32>,
}

impl Runner {
    async fn new(client: &mut WorkersClient<Channel>, args: Arc<Args>) -> Result<Self> {
        let mut w = scheduler::Worker {
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
        w.id = reg.worker_id;
        let prob_dist = Uniform::from(0..100);
        Ok(Runner {
            proto: w,
            args,
            prob_dist,
        })
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
        let die_roll = rand::thread_rng().sample(self.prob_dist);
        if die_roll < self.args.prob_build_failure {
            if let Err(err) = send_heartbeat(
                self.id(),
                client,
                b.id,
                Some(tonic::Status::aborted("synthetic failure")),
            )
            .await
            {
                eprintln!("Error sending heartbeat for synthetic build failure: {err:?}");
            }
            return;
        }
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);
        let jh = tokio::spawn(build_heartbeats(
            self.id(),
            client.clone(),
            Arc::clone(&self.args),
            b.clone(),
            rx,
        ));
        tokio::time::sleep(tokio::time::Duration::from_millis(b.sleep_ms)).await;
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

    async fn run(&self, client: &mut WorkersClient<Channel>) -> Result<()> {
        println!("Starting runner {}.", self.id());
        let mut idle_iterations: u64 = 0;
        loop {
            // self.args.prob_idle_failure
            let die_roll = rand::thread_rng().sample(self.prob_dist);
            if die_roll < self.args.prob_idle_failure {
                break;
            }
            if idle_iterations > 10 && idle_iterations % 10 == 0 {
                println!(
                    "Runner {} idle for {} iterations.",
                    self.id(),
                    idle_iterations
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
                            match err.code() {
                                tonic::Code::FailedPrecondition
                                | tonic::Code::InvalidArgument
                                | tonic::Code::NotFound
                                | tonic::Code::OutOfRange
                                | tonic::Code::PermissionDenied
                                | tonic::Code::Unimplemented => {
                                    anyhow::bail!(
                                        "runner {} aborting due to unrecoverable error: {err}",
                                        self.id()
                                    );
                                }
                                _ => {
                                    should_sleep = true;
                                    println!(
                                        "Runner {} encountered error accepting build: {err}",
                                        self.id()
                                    );
                                }
                            };
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
        Ok(())
    }
}

async fn send_heartbeat(
    runner_id: u64,
    client: &mut WorkersClient<Channel>,
    build_id: u64,
    result: Option<tonic::Status>,
) -> Result<()> {
    let (done, status, details) = match result {
        None => (false, 0, String::new()),
        Some(st) => (true, st.code().into(), st.message().to_owned()),
    };
    if let Err(err) = client
        .build_heart_beat(scheduler::BuildHeartBeatRequest {
            build_id,
            worker_id: runner_id,
            done,
            status,
            details,
        })
        .await
    {
        match err.code() {
            tonic::Code::FailedPrecondition | tonic::Code::InvalidArgument => {
                anyhow::bail!("runner {runner_id} heartbeat task for build {build_id} encountered unrecoverable error: {err}");
            }
            _ => {
                println!("Runner {runner_id} failed to send heartbeat with done={done} for build {build_id}: {err}");
            }
        }
    }
    Ok(())
}

async fn build_heartbeats(
    runner_id: u64,
    mut client: WorkersClient<Channel>,
    args: Arc<Args>,
    b: scheduler::Build,
    mut rx: tokio::sync::mpsc::Receiver<()>,
) {
    let dist = Uniform::from(0..100);
    loop {
        let die_roll = rand::thread_rng().sample(dist);
        if die_roll < args.prob_build_heartbeat_failure {
            return;
        }
        if tokio::time::timeout(tokio::time::Duration::from_secs(5), rx.recv())
            .await
            .is_err()
        {
            if let Err(err) = send_heartbeat(runner_id, &mut client, b.id, None).await {
                println!(
                    "Aborting heartbeats for build {} in runner {runner_id} due to unrecoverable error: {err:?}",
                    b.id
                );
                return;
            }
            continue;
        }
        // Received build completion signal on rx.
        break;
    }
    if let Err(err) =
        send_heartbeat(runner_id, &mut client, b.id, Some(tonic::Status::ok(""))).await
    {
        println!(
            "Unrecoverable error sending completion heartbeat for build {} in runner {runner_id}: {err:?}",
            b.id
        );
    }
}

async fn sleep_seconds(secs: u64) {
    tokio::time::sleep(tokio::time::Duration::from_secs(secs)).await
}

async fn launch_new_runner(task_id: i32, args: Arc<Args>) -> Result<()> {
    let mut client = WorkersClient::connect(args.scheduler_addr.clone())
        .await
        .with_context(|| {
            format!(
                "failed to connect to Workers service at address {}",
                args.scheduler_addr
            )
        })?;
    let r = Runner::new(&mut client, Arc::clone(&args))
        .await
        .with_context(|| "failed to initialize runner")?;
    r.run(&mut client)
        .await
        .with_context(|| format!("runner {} exited with error", r.id()))?;
    println!(
        "Runner {} in runner task {task_id} is shutting down.",
        r.id()
    );
    Ok(())
}

async fn runner_task(args: Arc<Args>, task_id: i32) {
    loop {
        println!("Runner task {task_id} launching new runner.");
        if let Err(err) = launch_new_runner(task_id, Arc::clone(&args)).await {
            println!("Runner in runner task {task_id} exited with error: {err:?}");
        } else {
            println!("Runner in runner task {task_id} exited without error.");
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}

fn check_prob_arg(val: i32, name: &str) -> Result<()> {
    if !(0..=100).contains(&val) {
        anyhow::bail!("invalid value for flag {name}, got {val}, want betweeen >= 0 and <= 100")
    }
    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.workers < 1 {
        anyhow::bail!(
            "invalid value for option workers, got {}, want >= 1",
            args.workers
        );
    }
    if args.scheduler_addr.is_empty() {
        anyhow::bail!("option scheduler flag can't be empty");
    }
    check_prob_arg(args.prob_build_failure, "prob-build-failure")?;
    check_prob_arg(
        args.prob_build_heartbeat_failure,
        "prob-build-heartbeat-failure",
    )?;
    check_prob_arg(args.prob_idle_failure, "prob-idle-failure")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arc::new(Args::parse());
    validate_args(&args).with_context(|| "error validating command line arguments")?;
    let mut js = tokio::task::JoinSet::new();
    println!(
        "Launching {} workers for scheduler at {}.",
        args.workers, args.scheduler_addr
    );
    for i in 0..args.workers {
        js.spawn(runner_task(Arc::clone(&args), i));
    }
    while let Some(r) = js.join_next().await {
        r.with_context(|| "runner crashed")?;
    }

    Ok(())
}
