use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    sync::{atomic::Ordering, Mutex, MutexGuard},
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
    registered_at: tokio::time::Instant,
    last_heartbeat_at: tokio::time::Instant,
    tx: Option<tokio::sync::oneshot::Sender<scheduler::Build>>,
    assigned_build: Option<u64>,
}

struct BuildSet {
    requirements: Constraints,
    queue: VecDeque<u64>,
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

pub enum BuildResponse {
    Build(scheduler::Build),
    WaitChannel(tokio::sync::oneshot::Receiver<scheduler::Build>),
}

fn req_into_constraints(req: &[scheduler::Requirement]) -> Constraints {
    req.iter()
        .map(|v| (v.key.clone(), v.value.clone()))
        .collect()
}

fn res_into_constraints(res: &[scheduler::Resource]) -> Constraints {
    res.iter()
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

fn get_or_create_worker_set(
    workersets: &mut HashMap<WorkerSetId, WorkerSet>,
    buildsets: &mut HashMap<BuildSetID, BuildSet>,
    res: &[scheduler::Resource],
) -> Result<WorkerSetId, tonic::Status> {
    let cons = res_into_constraints(res);
    let wsid = setid_from_constraints(&cons).map_err(|err| {
        tonic::Status::internal(format!(
            "error generating a fingerprint for worker resources: {err:?}"
        ))
    })?;
    let mut inserted = false;
    let ws = workersets.entry(wsid).or_insert_with(|| {
        inserted = true;
        WorkerSet {
            resources: cons,
            waiting_worker_ids: VecDeque::new(),
            build_set_ids: Vec::new(),
        }
    });

    if inserted {
        ws.add_compatible_build_sets(&wsid, buildsets);
    }
    Ok(wsid)
}

fn try_assign_build(
    build: &scheduler::Build,
    bs: &BuildSet,
    workersets: &mut HashMap<WorkerSetId, WorkerSet>,
    workers: &mut HashMap<u64, Worker>,
) -> Option<u64> {
    let build_id = build.id;
    for wsid in bs.worker_set_ids.iter() {
        let ws = if let Some(ws) = workersets.get_mut(wsid) {
            ws
        } else {
            println!("ERROR: Found non-existent worker set in build set while trying to assign build {build_id}: worker_set {wsid:?}");
            continue;
        };
        while let Some(wid) = ws.waiting_worker_ids.pop_front() {
            let w = if let Some(w) = workers.get_mut(&wid) {
                w
            } else {
                println!("ERROR: Found invalid worker ID {wid} in worker set {wsid:?} while trying to assign build {build_id}");
                continue;
            };
            let assigned = match w.try_assign_build(build) {
                Ok(a) => a,
                Err(err) => {
                    println!("ERROR: Unable to assign build {build_id} to worker {wid} in worker set {wsid:?}: {err:?}");
                    continue;
                }
            };
            if !assigned {
                continue;
            }
            return Some(wid);
        }
    }
    None
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

impl WorkerSet {
    fn add_compatible_build_sets(
        &mut self,
        wsid: &WorkerSetId,
        buildsets: &mut HashMap<BuildSetID, BuildSet>,
    ) {
        for (bsid, bs) in buildsets.iter_mut() {
            if !is_compatible(&bs.requirements, &self.resources) {
                continue;
            }
            self.build_set_ids.push(*bsid);
            bs.worker_set_ids.push(*wsid);
        }
    }

    fn add_available_worker(&mut self, worker_id: u64) {
        if self.waiting_worker_ids.iter().any(|&wid| wid == worker_id) {
            // Worker ID already exists in the waiting list. Probably from a
            // previous call to "accept_build".
            return;
        }
        self.waiting_worker_ids.push_back(worker_id);
    }
}

impl Worker {
    fn expired(&self) -> bool {
        self.last_heartbeat_at.elapsed() > tokio::time::Duration::from_secs(5 * 60)
    }

    fn check_assignment(&self, build_id: u64) -> Result<(), tonic::Status> {
        if let Some(prev_build_id) = self.assigned_build {
            if prev_build_id != build_id {
                return Err(tonic::Status::internal(format!("can't assign build {build_id} to worker because the worker is already assigned build {prev_build_id}")));
            }
        }
        Ok(())
    }

    fn try_assign_build(&mut self, build: &scheduler::Build) -> Result<bool, tonic::Status> {
        self.check_assignment(build.id)?;
        if let Some(tx) = self.tx.take() {
            if tx.send(build.clone()).is_err() {
                return Ok(false);
            }
            self.assigned_build = Some(build.id);
            return Ok(true);
        }
        Ok(false)
    }

    fn assign_build(&mut self, build: &scheduler::Build) -> Result<(), tonic::Status> {
        self.check_assignment(build.id)?;
        self.assigned_build = Some(build.id);
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(build.clone());
        }
        Ok(())
    }
}

impl Build {
    fn assign_worker(&mut self, worker_id: u64) -> Result<(), tonic::Status> {
        if let Some(prev_worker_id) = self.assigned_worker {
            if prev_worker_id != worker_id {
                return Err(tonic::Status::internal(format!("build was already previously assigned to worker {prev_worker_id} but is being assigned to {worker_id} again")));
            }
        } else {
            let now = tokio::time::Instant::now();
            self.assigned_at = Some(now);
            self.assigned_worker = Some(worker_id);
            self.last_heartbeat_at = Some(now);
        }
        if let Some(tx) = self.tx.take() {
            // The client may have stopped waiting so it's ok for
            // this send to fail.
            let _ = tx.send(());
        }
        Ok(())
    }
}

type QueueLocks<'a> = (
    MutexGuard<'a, HashMap<BuildSetID, BuildSet>>,
    MutexGuard<'a, HashMap<WorkerSetId, WorkerSet>>,
    MutexGuard<'a, HashMap<u64, Build>>,
    MutexGuard<'a, HashMap<u64, Worker>>,
);

impl Queue {
    pub fn new() -> Self {
        Queue {
            next_build_id: 1.into(),
            next_worker_id: 1.into(),
            builds: Mutex::new(HashMap::new()),
            workers: Mutex::new(HashMap::new()),
            buildsets: Mutex::new(HashMap::new()),
            workersets: Mutex::new(HashMap::new()),
        }
    }

    fn grab_locks(&self) -> Result<QueueLocks<'_>, tonic::Status> {
        let buildsets = self.buildsets.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler build set lock: {err:?}"
            ))
        })?;
        let workersets = self.workersets.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler worker set lock: {err:?}"
            ))
        })?;
        let builds = self.builds.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler builds lock: {err:?}"
            ))
        })?;
        let workers = self.workers.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler workers lock: {err:?}"
            ))
        })?;
        Ok((buildsets, workersets, builds, workers))
    }

    pub fn create_build(&self, mut sbuild: scheduler::Build) -> Result<u64, tonic::Status> {
        let (mut buildsets, mut workersets, mut builds, mut workers) = self.grab_locks()?;
        let (bsid, bs) =
            get_or_create_build_set(&mut buildsets, &mut workersets, &sbuild.requirements)?;
        let build_id = self.next_build_id.fetch_add(1, Ordering::Relaxed);
        sbuild.id = build_id;
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
        if let Some(worker_id) = try_assign_build(&build.proto, bs, &mut workersets, &mut workers) {
            build.assign_worker(worker_id)?;
        } else {
            bs.queue.push_back(build_id);
        }
        builds.insert(build_id, build);
        Ok(build_id)
    }

    pub fn register_worker(&self, res: &[scheduler::Resource]) -> Result<u64, tonic::Status> {
        let (mut buildsets, mut workersets, builds, mut workers) = self.grab_locks()?;
        drop(builds);
        let wsid = get_or_create_worker_set(&mut workersets, &mut buildsets, res)?;
        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        workers.insert(
            worker_id,
            Worker {
                wsid,
                registered_at: tokio::time::Instant::now(),
                last_heartbeat_at: tokio::time::Instant::now(),
                tx: None,
                assigned_build: None,
            },
        );
        Ok(worker_id)
    }

    pub fn accept_build(&self, worker_id: u64) -> Result<BuildResponse, tonic::Status> {
        let (mut buildsets, mut workersets, mut builds, mut workers) = self.grab_locks()?;
        let w = workers.get_mut(&worker_id).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("invalid worker id {worker_id}"))
        })?;
        if let Some(build_id) = w.assigned_build {
            // This worker has already been assigned a build
            let b = builds.get_mut(&build_id).ok_or_else(|| {
                tonic::Status::internal(format!(
                    "popped non-existent build id {build_id:?} from internal queue"
                ))
            })?;
            b.assign_worker(worker_id).map_err(|err| {
                tonic::Status::new(
                    err.code(),
                    format!("error assigning build {build_id} to worker {worker_id}"),
                )
            })?;
            return Ok(BuildResponse::Build(b.proto.clone()));
        }
        let ws = workersets.get_mut(&w.wsid).ok_or_else(|| {
            tonic::Status::internal(format!(
                "worker id {worker_id} referenced non-existent worker set with id {:?}",
                w.wsid
            ))
        })?;
        for bsid in ws.build_set_ids.iter() {
            let bs = buildsets.get_mut(bsid).ok_or_else(|| {
                tonic::Status::internal(format!(
                    "worker set for worker id {worker_id} referenced non-existent build set with id {:?}",
                    bsid,
                ))
            })?;
            let build_id = if let Some(build_id) = bs.queue.pop_front() {
                build_id
            } else {
                continue;
            };
            let b = builds.get_mut(&build_id).ok_or_else(|| {
                tonic::Status::internal(format!(
                    "popped non-existent build id {build_id:?} from internal queue"
                ))
            })?;
            b.assign_worker(worker_id).map_err(|err| {
                tonic::Status::new(
                    err.code(),
                    format!("error assigning build {build_id} to worker {worker_id}"),
                )
            })?;
            return Ok(BuildResponse::Build(b.proto.clone()));
        }

        // No queued build. Mark self as available and return a channel to wait on for a build
        // to be assigned to this worker.
        ws.add_available_worker(worker_id);
        let (tx, rx) = tokio::sync::oneshot::channel();
        w.tx = Some(tx);
        Ok(BuildResponse::WaitChannel(rx))
    }

    pub fn build_heartbeat(
        &self,
        worker_id: u64,
        build_id: u64,
        done: bool,
    ) -> Result<(), tonic::Status> {
        Err(tonic::Status::unimplemented(
            "build_heartbeat is unimplemented",
        ))
    }

    pub fn wait_build(&self, build_id: u64) -> Result<BuildResponse, tonic::Status> {
        Err(tonic::Status::unimplemented("wait_build is unimplemented"))
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
