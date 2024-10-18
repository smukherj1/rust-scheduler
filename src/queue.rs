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
    created_at: std::time::SystemTime,
    assigned_at: Option<std::time::SystemTime>,
    assigned_worker: Option<u64>,
    last_heartbeat_at: Option<std::time::SystemTime>,
    completed_at: Option<std::time::SystemTime>,
    status: Option<tonic::Status>,
    tx: Option<tokio::sync::oneshot::Sender<scheduler::BuildResult>>,
    proto: scheduler::Build,
}

struct Worker {
    wsid: WorkerSetId,
    registered_at: std::time::SystemTime,
    last_heartbeat_at: std::time::SystemTime,
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

pub struct QueueStats {
    pub total_builds: u64,
    pub queued_builds: u64,
    pub running_builds: u64,
    pub total_workers: u64,
    pub idle_workers: u64,
    pub busy_workers: u64,
}

pub struct Queue {
    next_build_id: std::sync::atomic::AtomicU64,
    next_worker_id: std::sync::atomic::AtomicU64,
    queued_builds: std::sync::atomic::AtomicU64,
    running_builds: std::sync::atomic::AtomicU64,
    idle_workers: std::sync::atomic::AtomicU64,
    busy_workers: std::sync::atomic::AtomicU64,
    total_builds: std::sync::atomic::AtomicU64,
    total_workers: std::sync::atomic::AtomicU64,

    builds: Mutex<HashMap<u64, Build>>,
    workers: Mutex<HashMap<u64, Worker>>,
    buildsets: Mutex<HashMap<BuildSetID, BuildSet>>,
    workersets: Mutex<HashMap<WorkerSetId, WorkerSet>>,
}

pub enum BuildResponse {
    Build(scheduler::Build),
    WaitChannel(tokio::sync::oneshot::Receiver<scheduler::Build>),
}

pub enum BuildResultResponse {
    BuildResult(scheduler::BuildResult),
    WaitChannel(tokio::sync::oneshot::Receiver<scheduler::BuildResult>),
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

    fn remove_available_worker(&mut self, worker_id: u64) {
        self.waiting_worker_ids.retain(|id| *id != worker_id);
    }
}

impl Worker {
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

    fn on_heartbeat(&mut self, build_id: u64, done: bool) -> Result<(), tonic::Status> {
        if let Some(assigned_build_id) = self.assigned_build {
            if assigned_build_id != build_id {
                return Err(tonic::Status::invalid_argument(format!("invalid heartbeat for build {build_id}, done={done} because worker is currently assigned build {assigned_build_id}")));
            }
        } else {
            return Err(tonic::Status::invalid_argument(format!("invalid heartbeat for build {build_id}, done={done} because worker is currently not assigned any build")));
        }
        let now = std::time::SystemTime::now();
        self.last_heartbeat_at = now;
        if done {
            self.assigned_build = None;
        }
        Ok(())
    }
}

impl Build {
    fn verify_assignment(&self, worker_id: u64) -> Result<(), tonic::Status> {
        if let Some(assigned_worker_id) = self.assigned_worker {
            if assigned_worker_id != worker_id {
                return Err(tonic::Status::invalid_argument(format!("can't modify build as worker {worker_id} because build is actually assigned to worker {assigned_worker_id}")));
            }
        } else {
            return Err(tonic::Status::invalid_argument(format!(
                "can't modify build as worker {worker_id} because build is currently unassigned"
            )));
        }
        Ok(())
    }
    fn on_heartbeat(&mut self, worker_id: u64, done: bool) -> Result<(), tonic::Status> {
        self.verify_assignment(worker_id)?;
        if let Some(st) = self.status.as_ref() {
            return Err(tonic::Status::invalid_argument(format!("unexpected heartbeat from worker {worker_id} for already completed build with status {st}")));
        }
        let now = std::time::SystemTime::now();
        self.last_heartbeat_at = Some(now);
        if !done {
            return Ok(());
        }
        self.completed_at = Some(now);
        self.status = Some(tonic::Status::ok(""));
        Ok(())
    }

    fn assign_worker(&mut self, worker_id: u64) -> Result<(), tonic::Status> {
        if let Some(prev_worker_id) = self.assigned_worker {
            if prev_worker_id != worker_id {
                return Err(tonic::Status::internal(format!("build was already previously assigned to worker {prev_worker_id} but is being assigned to {worker_id} again")));
            }
        } else {
            let now = std::time::SystemTime::now();
            self.assigned_at = Some(now);
            self.assigned_worker = Some(worker_id);
            self.last_heartbeat_at = Some(now);
        }
        Ok(())
    }

    fn completed(&self) -> bool {
        self.status.is_some()
    }

    fn result(&self) -> scheduler::BuildResult {
        let status = match self.status.as_ref() {
            None => tonic::Status::unknown("pending on worker"),
            Some(s) => s.clone(),
        };
        let as_time_ms = |t: std::time::SystemTime| {
            let dur: u64 = match t.duration_since(std::time::UNIX_EPOCH).ok() {
                None => 0,
                Some(d) => d.as_millis().try_into().unwrap_or(0),
            };
            dur
        };
        let from_opt_time = |tops: Option<std::time::SystemTime>| match tops {
            Some(t) => as_time_ms(t),
            None => 0,
        };
        scheduler::BuildResult {
            creation_time_ms: as_time_ms(self.created_at),
            assign_time_ms: from_opt_time(self.assigned_at),
            completion_time_ms: from_opt_time(self.completed_at),
            status: status.code().into(),
            details: if status.code() != tonic::Code::Ok {
                format!("{:?}", status)
            } else {
                String::new()
            },
        }
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
            queued_builds: 0.into(),
            running_builds: 0.into(),
            idle_workers: 0.into(),
            busy_workers: 0.into(),
            total_builds: 0.into(),
            total_workers: 0.into(),
            builds: Mutex::new(HashMap::new()),
            workers: Mutex::new(HashMap::new()),
            buildsets: Mutex::new(HashMap::new()),
            workersets: Mutex::new(HashMap::new()),
        }
    }

    pub fn stats(&self) -> QueueStats {
        QueueStats {
            total_builds: self.total_builds.load(Ordering::Relaxed),
            queued_builds: self.queued_builds.load(Ordering::Relaxed),
            running_builds: self.running_builds.load(Ordering::Relaxed),
            total_workers: self.total_workers.load(Ordering::Relaxed),
            idle_workers: self.idle_workers.load(Ordering::Relaxed),
            busy_workers: self.busy_workers.load(Ordering::Relaxed),
        }
    }

    fn grab_builds_lock(&self) -> Result<MutexGuard<HashMap<u64, Build>>, tonic::Status> {
        let builds = self.builds.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler builds lock: {err:?}"
            ))
        })?;
        self.total_builds
            .store(builds.len() as u64, Ordering::Relaxed);
        Ok(builds)
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
        let builds = self.grab_builds_lock()?;
        let workers = self.workers.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler workers lock: {err:?}"
            ))
        })?;
        self.total_workers
            .store(workers.len() as u64, Ordering::Relaxed);
        Ok((buildsets, workersets, builds, workers))
    }

    fn update_build_assignment_counters(&self, queued: bool) {
        if queued && self.queued_builds.fetch_sub(1, Ordering::Relaxed) == 0 {
            self.queued_builds.store(0, Ordering::Relaxed);
        }
        self.running_builds.fetch_add(1, Ordering::Relaxed);
        if self.idle_workers.fetch_sub(1, Ordering::Relaxed) == 0 {
            self.idle_workers.store(0, Ordering::Relaxed);
        }
        self.busy_workers.fetch_add(1, Ordering::Relaxed);
    }

    fn update_build_completion_counters(&self) {
        if self.busy_workers.fetch_sub(1, Ordering::Relaxed) == 0 {
            self.busy_workers.store(0, Ordering::Relaxed);
        }
        self.idle_workers.fetch_add(1, Ordering::Relaxed);
        if self.running_builds.fetch_sub(1, Ordering::Relaxed) == 0 {
            self.running_builds.store(0, Ordering::Relaxed);
        }
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
            created_at: std::time::SystemTime::now(),
            last_heartbeat_at: None,
            completed_at: None,
            status: None,
            tx: None,
            proto: sbuild,
        };
        if let Some(worker_id) = try_assign_build(&build.proto, bs, &mut workersets, &mut workers) {
            build.assign_worker(worker_id)?;
            self.update_build_assignment_counters(false);
        } else {
            self.queued_builds.fetch_add(1, Ordering::Relaxed);
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
                registered_at: std::time::SystemTime::now(),
                last_heartbeat_at: std::time::SystemTime::now(),
                tx: None,
                assigned_build: None,
            },
        );
        self.idle_workers.fetch_add(1, Ordering::Relaxed);
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
            w.assign_build(&b.proto).map_err(|err| {
                tonic::Status::new(
                    err.code(),
                    format!("error assigning build {build_id} to worker {worker_id}"),
                )
            })?;
            ws.remove_available_worker(worker_id);
            self.update_build_assignment_counters(true);
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
        let (buildsets, mut workersets, mut builds, mut workers) = self.grab_locks()?;
        drop(buildsets);
        let build = builds.get_mut(&build_id).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("build id {build_id} is invalid in heartbeat request from worker {worker_id}, done={done}"))
        })?;
        let worker = workers.get_mut(&worker_id).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("worker id {build_id} is invalid in heartbeat request for build {build_id}, done={done}"))
        })?;
        build.on_heartbeat(worker_id, done).map_err(|err| {
            tonic::Status::new(err.code(), format!("unable to record heartbeat on build {build_id} from worker {worker_id}, done={done}: {err:?}"))
        })?;
        worker.on_heartbeat(build_id, done).map_err(|err| {
            tonic::Status::new(err.code(), format!("unable to record heartbeat on worker {worker_id} for build {build_id}, done={done}: {err:?}"))
        })?;
        if !done {
            return Ok(());
        }
        if let Some(tx) = build.tx.take() {
            let _ = tx.send(build.result());
        }
        let ws = workersets.get_mut(&worker.wsid).ok_or_else(|| {
            tonic::Status::internal(format!(
                "worker id {worker_id} contained non-existent worker set id {:?}",
                worker.wsid
            ))
        })?;
        ws.add_available_worker(worker_id);
        self.update_build_completion_counters();
        Ok(())
    }

    pub fn wait_build(&self, build_id: u64) -> Result<BuildResultResponse, tonic::Status> {
        let mut builds = self.grab_builds_lock()?;
        let build = builds.get_mut(&build_id).ok_or_else(|| {
            tonic::Status::invalid_argument(format!(
                "can't wait on non-existent build id {build_id}"
            ))
        })?;
        if build.completed() {
            return Ok(BuildResultResponse::BuildResult(build.result()));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        build.tx = Some(tx);
        Ok(BuildResultResponse::WaitChannel(rx))
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
