use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    sync::{atomic::Ordering, Mutex, MutexGuard},
    time::Duration,
};

use crate::metrics;

pub mod scheduler {
    tonic::include_proto!("scheduler");
}

const WORKER_LOST_THRESHOLD_SECONDS: u64 = 60;
const BUILD_EXPIRED_THRESHOLD_SECONDS: u64 = 60;
const WORKER_SET_EXPIRY_THRESHOLD_SECONDS: u64 = 60;
const BUILD_SET_EXPIRY_THRESHOLD_SECONDS: u64 = 60;

type Constraints = HashMap<String, String>;
type SetID = [u8; 32];
type BuildSetID = SetID;
type WorkerSetID = SetID;

#[derive(Eq, PartialEq)]
enum BuildState {
    Queued,
    Running,
    Complete,
}

struct Build {
    bsid: BuildSetID,
    created_at: std::time::SystemTime,
    assigned_at: Option<std::time::SystemTime>,
    assigned_worker: Option<u64>,
    last_heartbeat_at: Option<std::time::SystemTime>,
    completed_at: Option<std::time::SystemTime>,
    status: Option<tonic::Status>,
    state: BuildState,
    tx: Option<tokio::sync::oneshot::Sender<scheduler::BuildResult>>,
    proto: scheduler::Build,
}

struct Worker {
    wsid: WorkerSetID,
    last_heartbeat_at: std::time::SystemTime,
    tx: Option<tokio::sync::oneshot::Sender<scheduler::Build>>,
    assigned_build: Option<u64>,
}

struct BuildSet {
    requirements: Constraints,
    queue: VecDeque<u64>,
    worker_set_ids: Vec<WorkerSetID>,
    last_touched: std::time::Instant,
}

struct WorkerSet {
    resources: Constraints,
    waiting_worker_ids: VecDeque<u64>,
    build_set_ids: Vec<BuildSetID>,
    last_touched: std::time::Instant,
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
    max_builds: usize,
    max_workers: usize,
    max_buildsets: usize,
    max_workersets: usize,

    builds: Mutex<Builds>,
    workers: Mutex<Workers>,
    buildsets: Mutex<BuildSets>,
    workersets: Mutex<WorkerSets>,
}

pub enum BuildResponse {
    Build(scheduler::Build),
    WaitChannel(tokio::sync::oneshot::Receiver<scheduler::Build>),
}

pub enum BuildResultResponse {
    BuildResult(scheduler::BuildResult),
    WaitChannel(tokio::sync::oneshot::Receiver<scheduler::BuildResult>),
}

struct LockReporter<'a> {
    name: &'a str,
    begin: std::time::Instant,
}

impl<'a> LockReporter<'a> {
    fn new(name: &'a str) -> Self {
        metrics::LOCK_WAITERS.with_label_values(&[name]).inc();
        LockReporter {
            name,
            begin: std::time::Instant::now(),
        }
    }
}

impl<'a> Drop for LockReporter<'a> {
    fn drop(&mut self) {
        metrics::LOCK_WAITERS.with_label_values(&[self.name]).dec();
        metrics::LOCK_WAIT_LATENCY
            .with_label_values(&[self.name])
            .observe(
                std::time::Instant::now()
                    .checked_duration_since(self.begin)
                    .unwrap_or(Duration::from_millis(0))
                    .as_millis() as f64,
            );
    }
}

struct GCReporter {
    begin: std::time::Instant,
}

impl GCReporter {
    fn new() -> Self {
        GCReporter {
            begin: std::time::Instant::now(),
        }
    }
}

impl Drop for GCReporter {
    fn drop(&mut self) {
        metrics::GC_CYCLES.inc();
        metrics::GC_LATENCY.observe(
            std::time::Instant::now()
                .checked_duration_since(self.begin)
                .unwrap_or(Duration::from_millis(0))
                .as_millis() as f64,
        );
    }
}

fn report_build_result(r: &scheduler::BuildResult) {
    let code = metrics::code_str(tonic::Code::from_i32(r.status));
    metrics::BUILDS_COMPLETED_COUNT
        .with_label_values(&[code])
        .inc();
    let queue_ms = if r.assign_time_ms > r.creation_time_ms {
        r.assign_time_ms - r.creation_time_ms
    } else {
        0
    };
    metrics::BUILD_QUEUE_LATENCY
        .with_label_values(&[code])
        .observe(queue_ms as f64);
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
    workersets: &mut HashMap<WorkerSetID, WorkerSet>,
    req: &[scheduler::Requirement],
    max_buildsets: usize,
) -> Result<(BuildSetID, &'a mut BuildSet), tonic::Status> {
    let cons = req_into_constraints(req);
    let bsid: BuildSetID = setid_from_constraints(&cons).map_err(|err| {
        tonic::Status::internal(format!(
            "error generating a fingerprint for build requirements: {err:?}"
        ))
    })?;
    if !buildsets.contains_key(&bsid) && buildsets.len() >= max_buildsets {
        return Err(tonic::Status::resource_exhausted(format!(
            "exceeded limit {max_buildsets} for groups of builds with unique requirements"
        )));
    }
    let mut inserted = false;
    let bs = buildsets.entry(bsid).or_insert_with(|| {
        inserted = true;
        BuildSet {
            requirements: cons,
            queue: VecDeque::new(),
            worker_set_ids: Vec::new(),
            last_touched: std::time::Instant::now(),
        }
    });
    if inserted {
        bs.add_compatible_worker_sets(&bsid, workersets)?;
    } else if bs.worker_set_ids.is_empty() {
        return Err(tonic::Status::failed_precondition(
            "no available worker matches the given requirements",
        ));
    } else {
        bs.touch();
    }
    Ok((bsid, bs))
}

fn get_or_create_worker_set(
    workersets: &mut HashMap<WorkerSetID, WorkerSet>,
    buildsets: &mut HashMap<BuildSetID, BuildSet>,
    res: &[scheduler::Resource],
    max_workersets: usize,
) -> Result<WorkerSetID, tonic::Status> {
    let cons = res_into_constraints(res);
    let wsid = setid_from_constraints(&cons).map_err(|err| {
        tonic::Status::internal(format!(
            "error generating a fingerprint for worker resources: {err:?}"
        ))
    })?;
    if !workersets.contains_key(&wsid) && workersets.len() > max_workersets {
        return Err(tonic::Status::resource_exhausted(format!(
            "exceeded limit {max_workersets} for groups of workers with unique resources"
        )));
    }
    let mut inserted = false;
    let ws = workersets.entry(wsid).or_insert_with(|| {
        inserted = true;
        WorkerSet {
            resources: cons,
            waiting_worker_ids: VecDeque::new(),
            build_set_ids: Vec::new(),
            last_touched: std::time::Instant::now(),
        }
    });

    if inserted {
        ws.add_compatible_build_sets(&wsid, buildsets);
    } else {
        ws.touch();
    }
    Ok(wsid)
}

fn try_assign_build(
    build: &scheduler::Build,
    bs: &BuildSet,
    workersets: &mut HashMap<WorkerSetID, WorkerSet>,
    workers: &mut HashMap<u64, Worker>,
) -> Option<u64> {
    let build_id = build.id;
    for wsid in bs.worker_set_ids.iter() {
        let ws = if let Some(ws) = workersets.get_mut(wsid) {
            ws
        } else {
            println!("ERROR: Found non-existent worker set in build set while trying to assign build {build_id}: worker_set {wsid:x?}");
            continue;
        };
        while let Some(wid) = ws.waiting_worker_ids.pop_front() {
            let w = if let Some(w) = workers.get_mut(&wid) {
                w
            } else {
                println!("ERROR: Found invalid worker ID {wid} in worker set {wsid:x?} while trying to assign build {build_id}");
                continue;
            };
            let assigned = match w.try_assign_build(build) {
                Ok(a) => a,
                Err(err) => {
                    println!("ERROR: Unable to assign build {build_id} to worker {wid} in worker set {wsid:x?}: {err:?}");
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

fn touch_build_set(bsid: &BuildSetID, buildsets: &mut BuildSets) -> Result<(), tonic::Status> {
    let bs = buildsets
        .get_mut(bsid)
        .ok_or_else(|| tonic::Status::internal(format!("invalid build set id {bsid:x?}")))?;
    bs.touch();
    Ok(())
}

fn touch_worker_set(wsid: &WorkerSetID, workersets: &mut WorkerSets) -> Result<(), tonic::Status> {
    let ws = workersets
        .get_mut(wsid)
        .ok_or_else(|| tonic::Status::internal(format!("invalid worker set id {wsid:x?}")))?;
    ws.touch();
    Ok(())
}

impl BuildSet {
    fn add_compatible_worker_sets(
        &mut self,
        bsid: &BuildSetID,
        workersets: &mut HashMap<WorkerSetID, WorkerSet>,
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

    fn remove_worker_set(&mut self, wsid: &WorkerSetID) {
        self.worker_set_ids.retain(|id| *id != *wsid);
    }

    fn on_expiry(&self, bsid: &BuildSetID, workersets: &mut WorkerSets, builds: &mut Builds) {
        for wsid in self.worker_set_ids.iter() {
            if let Some(ws) = workersets.get_mut(wsid) {
                ws.remove_build_set(bsid);
            }
        }
    }

    fn fail_unrunnable_builds(&mut self, builds: &mut Builds) {
        if !self.worker_set_ids.is_empty() {
            return;
        }
        while let Some(build_id) = self.queue.pop_front() {
            let build = if let Some(build) = builds.get_mut(&build_id) {
                build
            } else {
                continue;
            };
            build.fail(tonic::Status::failed_precondition("no matching worker"));
        }
    }

    fn touch(&mut self) {
        self.last_touched = std::time::Instant::now();
    }

    fn expired(&self) -> bool {
        self.queue.is_empty()
            && std::time::Instant::now()
                .checked_duration_since(self.last_touched)
                .unwrap_or(Duration::from_secs(0))
                .as_secs()
                > BUILD_SET_EXPIRY_THRESHOLD_SECONDS
    }
}

impl WorkerSet {
    fn add_compatible_build_sets(
        &mut self,
        wsid: &WorkerSetID,
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

    fn remove_build_set(&mut self, bsid: &BuildSetID) {
        self.build_set_ids.retain(|id| *id != *bsid);
    }

    fn on_expiry(&self, wsid: &WorkerSetID, buildsets: &mut BuildSets) {
        for bsid in self.build_set_ids.iter() {
            if let Some(bs) = buildsets.get_mut(bsid) {
                bs.remove_worker_set(wsid);
            }
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

    fn touch(&mut self) {
        self.last_touched = std::time::Instant::now();
    }

    fn expired(&self) -> bool {
        std::time::Instant::now()
            .checked_duration_since(self.last_touched)
            .unwrap_or(Duration::from_secs(0))
            .as_secs()
            > WORKER_SET_EXPIRY_THRESHOLD_SECONDS
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

    fn lost(&self) -> bool {
        if let Ok(dur) = self.last_heartbeat_at.elapsed() {
            return dur.as_secs() > WORKER_LOST_THRESHOLD_SECONDS;
        }
        false
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
    fn on_heartbeat(
        &mut self,
        worker_id: u64,
        done: bool,
        status: i32,
        details: String,
    ) -> Result<(), tonic::Status> {
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
        self.status = Some(tonic::Status::new(tonic::Code::from_i32(status), details));
        self.state = BuildState::Complete;
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
            self.state = BuildState::Running;
        }
        Ok(())
    }

    fn fail(&mut self, status: tonic::Status) {
        self.completed_at = Some(std::time::SystemTime::now());
        self.status = Some(status);
        self.state = BuildState::Complete;
    }

    fn completed(&self) -> bool {
        self.state == BuildState::Complete
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
            details: status.message().to_string(),
        }
    }

    fn expired(&self) -> bool {
        if let Some(completed_at) = self.completed_at {
            return completed_at
                .elapsed()
                .unwrap_or(std::time::Duration::from_secs(0))
                .as_secs()
                > BUILD_EXPIRED_THRESHOLD_SECONDS;
        }
        false
    }
}

type BuildSets = HashMap<BuildSetID, BuildSet>;
type WorkerSets = HashMap<WorkerSetID, WorkerSet>;
type Builds = HashMap<u64, Build>;
type Workers = HashMap<u64, Worker>;
type QueueLocks<'a> = (
    MutexGuard<'a, BuildSets>,
    MutexGuard<'a, WorkerSets>,
    MutexGuard<'a, Builds>,
    MutexGuard<'a, Workers>,
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
            max_builds: 1_000_000,
            max_workers: 1_000,
            max_buildsets: 500,
            max_workersets: 500,
            builds: Mutex::new(Builds::new()),
            workers: Mutex::new(Workers::new()),
            buildsets: Mutex::new(BuildSets::new()),
            workersets: Mutex::new(WorkerSets::new()),
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

    fn grab_builds_lock_inner(&self) -> Result<MutexGuard<Builds>, tonic::Status> {
        let builds = self.builds.lock().map_err(|err| {
            tonic::Status::internal(format!(
                "detected panic while trying to grab scheduler builds lock: {err:?}"
            ))
        })?;
        self.total_builds
            .store(builds.len() as u64, Ordering::Relaxed);
        Ok(builds)
    }

    fn grab_builds_lock(&self) -> Result<MutexGuard<Builds>, tonic::Status> {
        let _ = LockReporter::new("builds");
        self.grab_builds_lock_inner()
    }

    fn grab_locks(&self) -> Result<QueueLocks<'_>, tonic::Status> {
        let _ = LockReporter::new("all");
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
        let builds = self.grab_builds_lock_inner()?;
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

    fn check_builds_quota(&self, builds: &Builds) -> Result<(), tonic::Status> {
        if builds.len() > self.max_builds {
            return Err(tonic::Status::resource_exhausted(format!(
                "exceeded max builds capacity {}",
                self.max_builds
            )));
        }
        Ok(())
    }

    pub fn create_build(&self, mut sbuild: scheduler::Build) -> Result<u64, tonic::Status> {
        let (mut buildsets, mut workersets, mut builds, mut workers) = self.grab_locks()?;
        self.check_builds_quota(&builds)?;
        let (bsid, bs) = get_or_create_build_set(
            &mut buildsets,
            &mut workersets,
            &sbuild.requirements,
            self.max_buildsets,
        )?;
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
            state: BuildState::Queued,
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

    fn check_workers_quota(&self, workers: &Workers) -> Result<(), tonic::Status> {
        if workers.len() >= self.max_workers {
            return Err(tonic::Status::resource_exhausted(format!(
                "limit {} exceeded for active workers",
                self.max_workers
            )));
        }
        Ok(())
    }

    pub fn register_worker(&self, res: &[scheduler::Resource]) -> Result<u64, tonic::Status> {
        let (mut buildsets, mut workersets, builds, mut workers) = self.grab_locks()?;
        drop(builds);
        self.check_workers_quota(&workers)?;
        let wsid =
            get_or_create_worker_set(&mut workersets, &mut buildsets, res, self.max_workersets)?;
        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        workers.insert(
            worker_id,
            Worker {
                wsid,
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
                "worker id {worker_id} referenced non-existent worker set with id {:x?}",
                w.wsid
            ))
        })?;
        ws.touch();
        for bsid in ws.build_set_ids.iter() {
            let bs = buildsets.get_mut(bsid).ok_or_else(|| {
                tonic::Status::internal(format!(
                    "worker set for worker id {worker_id} referenced non-existent build set with id {:x?}",
                    bsid,
                ))
            })?;
            let build_id = if let Some(build_id) = bs.queue.pop_front() {
                build_id
            } else {
                continue;
            };
            bs.touch();
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
        status: i32,
        details: String,
    ) -> Result<(), tonic::Status> {
        let (mut buildsets, mut workersets, mut builds, mut workers) = self.grab_locks()?;
        let build = builds.get_mut(&build_id).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("build id {build_id} is invalid in heartbeat request from worker {worker_id}, done={done}"))
        })?;
        let worker = workers.get_mut(&worker_id).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("worker id {build_id} is invalid in heartbeat request for build {build_id}, done={done}"))
        })?;
        build.on_heartbeat(worker_id, done, status, details).map_err(|err| {
            tonic::Status::new(err.code(), format!("unable to record heartbeat on build {build_id} from worker {worker_id}, done={done}: {err:?}"))
        })?;
        worker.on_heartbeat(build_id, done).map_err(|err| {
            tonic::Status::new(err.code(), format!("unable to record heartbeat on worker {worker_id} for build {build_id}, done={done}: {err:?}"))
        })?;
        touch_build_set(&build.bsid, &mut buildsets).map_err(|err| {
            tonic::Status::new(err.code(), format!("unable to record heartbeat on build {build_id} from worker {worker_id}, done={done}: {err:?}"))
        })?;
        touch_worker_set(&worker.wsid, &mut workersets).map_err(|err| {
            tonic::Status::new(err.code(), format!("unable to record heartbeat on build {build_id} from worker {worker_id}, done={done}: {err:?}"))
        })?;
        if !done {
            return Ok(());
        }
        let build_result = build.result();
        report_build_result(&build_result);
        if let Some(tx) = build.tx.take() {
            let _ = tx.send(build_result);
        }
        let ws = workersets.get_mut(&worker.wsid).ok_or_else(|| {
            tonic::Status::internal(format!(
                "worker id {worker_id} contained non-existent worker set id {:x?}",
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

    fn gc_workers(&self, workers: &mut Workers, builds: &mut Builds) {
        let (mut idle_workers, mut busy_workers) = (0u64, 0u64);
        workers.retain(|wid, worker| {
            if !worker.lost() {
                if worker.assigned_build.is_some() {
                    busy_workers += 1;
                } else {
                    idle_workers += 1;
                }
                return true;
            }
            let assigned_build = if let Some(build_id) = worker.assigned_build {
                build_id
            } else {
                return false;
            };
            let build = match builds.get_mut(&assigned_build) {
                Some(build) => build,
                None => {
                    eprintln!("Error: During garbage collection, found worker {wid} assigned to non-existent build {assigned_build}.");
                    return false;
                },
            };
            build.fail(tonic::Status::aborted(format!(
                "assigned worker {wid} was lost",
            )));
            false
        });
        self.idle_workers.store(idle_workers, Ordering::Relaxed);
        self.busy_workers.store(busy_workers, Ordering::Relaxed);
    }
    fn gc_builds(&self, builds: &mut Builds) {
        let (mut queued_builds, mut running_builds) = (0u64, 0u64);
        builds.retain(|_, build| {
            if build.expired() {
                return false;
            }
            match build.state {
                BuildState::Queued => queued_builds += 1,
                BuildState::Running => running_builds += 1,
                BuildState::Complete => (),
            }
            true
        });
        self.queued_builds.store(queued_builds, Ordering::Relaxed);
        self.running_builds.store(running_builds, Ordering::Relaxed);
    }
    fn gc_buildsets(
        &self,
        buildsets: &mut BuildSets,
        workersets: &mut WorkerSets,
        builds: &mut Builds,
    ) {
        buildsets.retain(|bsid, bs| {
            if bs.expired() {
                bs.on_expiry(bsid, workersets, builds);
                return false;
            }
            bs.fail_unrunnable_builds(builds);
            bs.queue.retain(|build_id| builds.contains_key(build_id));
            true
        });
    }
    fn gc_workersets(
        &self,
        buildsets: &mut BuildSets,
        workersets: &mut WorkerSets,
        workers: &Workers,
    ) {
        workersets.retain(|wsid, ws| {
            if ws.expired() {
                ws.on_expiry(wsid, buildsets);
                return false;
            }
            ws.waiting_worker_ids
                .retain(|worker_id| workers.contains_key(worker_id));
            true
        });
    }

    pub fn gc(&self) -> Result<()> {
        let _ = GCReporter::new();
        let (mut buildsets, mut workersets, mut builds, mut workers) = self.grab_locks()?;
        self.gc_workers(&mut workers, &mut builds);
        self.gc_builds(&mut builds);
        self.gc_buildsets(&mut buildsets, &mut workersets, &mut builds);
        self.gc_workersets(&mut buildsets, &mut workersets, &workers);
        Ok(())
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
