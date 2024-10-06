use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    sync::{atomic::Ordering, Mutex},
};

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

type Constraints = HashMap<String, String>;
type SetID = [u8; 32];
type BuildSetID = SetID;
type WorkerSetId = SetID;

struct Build {
    bsid: BuildSetID,
    created_at: tokio::time::Instant,
    assigned_at: Option<tokio::time::Instant>,
    assigned_worker: Option<u64>,
    last_heartbeat_at: Option<tokio::time::Instant>,
    status: Option<tonic::Status>,
    tx: Option<tokio::sync::oneshot::Sender<()>>,
    proto: scheduler::Build,
}

struct Worker {
    wsid: WorkerSetId,
    registered_at: Option<tokio::time::Instant>,
    last_heartbeat_at: Option<tokio::time::Instant>,
    tx: Option<tokio::sync::oneshot::Sender<()>>,
    assigned_build: Option<u64>,
}

struct BuildSet {
    requirements: Constraints,
    queue: VecDeque<scheduler::Build>,
    worker_set_ids: Vec<WorkerSetId>,
}

struct WorkerSet {
    resources: Constraints,
    waiting_worker_ids: VecDeque<u64>,
    build_set_ids: Vec<BuildSetID>,
}

pub struct Queue {
    next_build_id: std::sync::atomic::AtomicU64,
    next_worker_id: std::sync::atomic::AtomicU64,
    builds: Mutex<HashMap<u64, Build>>,
    workers: Mutex<HashMap<u64, Worker>>,
    buildsets: Mutex<HashMap<BuildSetID, BuildSet>>,
    workersets: Mutex<HashMap<WorkerSetId, WorkerSet>>,
}

fn req_into_constraints(req: &[scheduler::Requirement]) -> Constraints {
    req.into_iter()
        .map(|v| (v.key.clone(), v.value.clone()))
        .collect()
}

fn res_into_constraints(res: &[scheduler::Requirement]) -> Constraints {
    res.into_iter()
        .map(|v| (v.key.clone(), v.value.clone()))
        .collect()
}

fn setid_from_constraints(cons: &HashMap<String, String>) -> Result<SetID> {
    let mut keys: Vec<&str> = cons.iter().map(|item| item.0.as_str()).collect();
    keys.sort();
    let mut h = Sha256::new();
    for k in keys.iter() {
        h.write(k.as_bytes())
            .with_context(|| format!("error hashing constraint key {k}"))?;
        // Keys assumed to exist in "cons" given we iterated over "cons" above to
        // generate "keys".
        let v = cons.get(*k).unwrap();
        h.write(v.as_bytes())
            .with_context(|| format!("error hashing constaint value {v} for key {k}"))?;
    }
    Ok(h.finalize().into())
}

fn is_compatible(req: &HashMap<String, String>, res: &HashMap<String, String>) -> bool {
    for (k, v) in req {
        match res.get(k) {
            None => return false,
            Some(res_v) => {
                if v != res_v {
                    return false;
                }
            }
        };
    }
    true
}

fn get_or_create_build_set<'a>(
    buildsets: &'a mut HashMap<BuildSetID, BuildSet>,
    workersets: &mut HashMap<WorkerSetId, WorkerSet>,
    req: &[scheduler::Requirement],
) -> Result<(BuildSetID, &'a mut BuildSet), tonic::Status> {
    let cons = req_into_constraints(req);
    let bsid: BuildSetID = setid_from_constraints(&cons).map_err(|err| {
        tonic::Status::internal(format!(
            "error generating a fingerprint for build requirements: {err:?}"
        ))
    })?;
    let bs = buildsets.entry(bsid).or_insert(BuildSet {
        requirements: cons,
        queue: VecDeque::new(),
        worker_set_ids: Vec::new(),
    });
    if bs.worker_set_ids.is_empty() {
        bs.add_compatible_worker_sets(&bsid, workersets)?;
    }
    Ok((bsid, bs))
}

impl BuildSet {
    fn add_compatible_worker_sets(
        &mut self,
        bsid: &BuildSetID,
        workersets: &mut HashMap<WorkerSetId, WorkerSet>,
    ) -> Result<(), tonic::Status> {
        for (wsid, ws) in workersets.iter_mut() {
            if !is_compatible(&self.requirements, &ws.resources) {
                continue;
            }
            ws.build_set_ids.push(*bsid);
            self.worker_set_ids.push(*wsid);
        }
        if self.worker_set_ids.is_empty() {
            return Err(tonic::Status::failed_precondition(
                "no available worker matches the given requirements",
            ));
        }
        Ok(())
    }
}

impl Queue {
    pub fn new() -> Self {
        Queue {
            next_build_id: 0.into(),
            next_worker_id: 0.into(),
            builds: Mutex::new(HashMap::new()),
            workers: Mutex::new(HashMap::new()),
            buildsets: Mutex::new(HashMap::new()),
            workersets: Mutex::new(HashMap::new()),
        }
    }

    pub fn create_build(&self, sbuild: scheduler::Build) -> Result<u64, tonic::Status> {
        let mut buildsets = self.buildsets.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler build set lock: {err:?}"
            ))
        })?;
        let mut workersets = self.workersets.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler worker set lock: {err:?}"
            ))
        })?;
        let (bsid, bs) =
            get_or_create_build_set(&mut buildsets, &mut workersets, &sbuild.requirements)?;
        let build_id = self.next_build_id.fetch_add(1, Ordering::Relaxed);
        let mut build = Build {
            bsid,
            assigned_at: None,
            assigned_worker: None,
            created_at: tokio::time::Instant::now(),
            last_heartbeat_at: None,
            status: None,
            tx: None,
            proto: sbuild,
        };
        Ok(build_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_succeeds() {
        Queue::new();
    }
}
