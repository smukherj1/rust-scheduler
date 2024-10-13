use anyhow::{Context, Result};
use clap::Parser;
use rand::Rng;
use scheduler::builds_client::BuildsClient;
use std::sync::Arc;

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
}

async fn loadgen_task_inner(args: &Args, task_id: i32) -> Result<()> {
    println!("Load generator task {task_id} is alive.");
    let mut client = BuildsClient::connect(args.scheduler_addr.clone())
        .await
        .with_context(|| format!("unable to connect to scheduler at {}", args.scheduler_addr))?;
    let mut builds = Vec::new();
    for _ in 0..10 {
        let dur = rand::thread_rng().gen_range(1..10u64);
        let resp = client
            .create_build(scheduler::CreateBuildRequest {
                build: Some(scheduler::Build {
                    id: 0,
                    requirements: vec![],
                    sleep_ms: dur * 1000,
                }),
            })
            .await
            .with_context(|| format!("task {task_id} failed to create build"))?
            .into_inner();
        builds.push(resp.build_id);
        println!(
            "Task {task_id} created build {} with duration {dur}s.",
            resp.build_id
        );
    }
    for build_id in builds {
        let resp = client
            .wait_build(scheduler::WaitBuildRequest { build_id })
            .await
            .with_context(|| format!("Task {task_id} got error waiting for build {build_id}"))?
            .into_inner();
        let build_result = match resp.build_result {
            None => {
                eprintln!("Task {task_id} got empty build result for build {build_id}");
                continue;
            }
            Some(r) => r,
        };
        print!(
            "Task {task_id}, build {build_id} completed with status={}",
            build_result.status
        );
    }
    println!("Load generator task {task_id} is exiting.");
    Ok(())
}

async fn loadgen_task(args: Arc<Args>, task_id: i32) {
    loop {
        if let Err(err) = loadgen_task_inner(&args, task_id).await {
            println!("Load generator task {task_id} quit with error: {err:?}",);
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
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
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arc::new(Args::parse());
    validate_args(&args).with_context(|| "error validating command line arguments")?;
    let mut js = tokio::task::JoinSet::new();
    println!(
        "Launching {} build load generators for scheduler at {}.",
        args.workers, args.scheduler_addr
    );
    for i in 0..args.workers {
        js.spawn(loadgen_task(Arc::clone(&args), i));
    }
    while let Some(r) = js.join_next().await {
        r.with_context(|| "load generator task crashed")?;
    }

    Ok(())
}
