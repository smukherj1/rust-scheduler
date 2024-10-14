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

    /// Number of concurrent builds to spawn per task.
    #[arg(short, long, default_value_t = 1)]
    builds_per_worker: i32,
}

struct Build {
    id: u64,
    sleep_ms: u64,
}

async fn loadgen_task_inner(args: &Args, task_id: i32) -> Result<()> {
    // println!("Load generator task {task_id} is alive.");
    let mut client = BuildsClient::connect(args.scheduler_addr.clone())
        .await
        .with_context(|| format!("unable to connect to scheduler at {}", args.scheduler_addr))?;
    let mut builds = Vec::new();
    for _ in 0..args.builds_per_worker {
        let dur = rand::thread_rng().gen_range(500..5000u64);
        let resp = client
            .create_build(scheduler::CreateBuildRequest {
                build: Some(scheduler::Build {
                    id: 0,
                    requirements: vec![],
                    sleep_ms: dur,
                }),
            })
            .await
            .with_context(|| format!("task {task_id} failed to create build"))?
            .into_inner();
        builds.push(Build {
            id: resp.build_id,
            sleep_ms: dur,
        });
        /*
        println!(
            "Task {task_id} created build {} with duration {dur}ms.",
            resp.build_id
        );
        */
    }
    for b in builds {
        let resp = client
            .wait_build(scheduler::WaitBuildRequest { build_id: b.id })
            .await
            .with_context(|| format!("Task {task_id} got error waiting for build {}", b.id))?
            .into_inner();
        let build_result = match resp.build_result {
            None => {
                eprintln!("Task {task_id} got empty build result for build {}", b.id);
                continue;
            }
            Some(r) => r,
        };
        let status = tonic::Status::new(
            tonic::Code::from_i32(build_result.status),
            build_result.details,
        );
        let queued_ms = if build_result.assign_time_ms > build_result.creation_time_ms {
            build_result.assign_time_ms - build_result.creation_time_ms
        } else {
            0
        };
        let exec_ms = if build_result.completion_time_ms > build_result.assign_time_ms {
            build_result.completion_time_ms - build_result.assign_time_ms
        } else {
            0
        };

        if status.code() != tonic::Code::Ok || b.id % 1000 == 0 || queued_ms > 10000 {
            println!(
                "Task {task_id}, build {}, sleep_ms {}, completed with status {} {}, queued_ms {queued_ms}, exec_ms {exec_ms}",
                b.id, b.sleep_ms, status.code(), status.message(),
            );
        }
    }
    // println!("Load generator task {task_id} is exiting.");
    Ok(())
}

async fn loadgen_task(args: Arc<Args>, task_id: i32) {
    loop {
        if let Err(err) = loadgen_task_inner(&args, task_id).await {
            eprintln!("Load generator task {task_id} quit with error: {err:?}",);
        }
        // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}

fn validate_args(args: &Args) -> Result<()> {
    if args.workers < 1 {
        anyhow::bail!(
            "invalid value for option workers, got {}, want >= 1",
            args.workers
        );
    }
    if args.builds_per_worker < 1 {
        anyhow::bail!(
            "invalid value for option builds_per_worker, got {}, want >= 1",
            args.builds_per_worker
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
