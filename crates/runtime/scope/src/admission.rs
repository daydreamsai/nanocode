use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use nanocode_durability_core::StoreError;
use nanocode_sync::{AcquireError, Notify, Semaphore, SemaphorePermit};

use crate::sim::{self, Mutation};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Admitted {
    pub(crate) admissions: u64,
    pub(crate) busy: u64,
    pub(crate) backfill_reads: u64,
}

pub(crate) struct Admission {
    reads: Semaphore,
    capacity: u32,
    admit_ms: u64,
    serving: AtomicU64,
    backfilling: AtomicU64,
    freed: Notify,
    admissions: AtomicU64,
    busy: AtomicU64,
    backfill_reads: AtomicU64,
}

struct Demand<'a>(&'a Admission);

impl Drop for Demand<'_> {
    fn drop(&mut self) {
        let before = self.0.serving.fetch_sub(1, Ordering::SeqCst);
        assert!(before > 0, "a serving read leaves the demand it joined");
        self.0.freed.notify_waiters();
    }
}

struct Lane<'a> {
    admission: &'a Admission,
    permit: Option<SemaphorePermit<'a>>,
}

impl Drop for Lane<'_> {
    fn drop(&mut self) {
        drop(self.permit.take());
        let before = self.admission.backfilling.fetch_sub(1, Ordering::SeqCst);
        assert!(before > 0, "a backfill read leaves the lane it took");
        self.admission.freed.notify_waiters();
    }
}

impl Admission {
    pub(crate) fn new(capacity: u32, admit_ms: u64) -> Self {
        assert!(
            admit_ms > 0,
            "a read waits for admission a while before Busy"
        );
        assert!(capacity > 0, "a host admits at least one read at a time");
        let permits = usize::try_from(capacity).expect("a read capacity fits in usize");
        Self {
            reads: Semaphore::new(permits),
            capacity,
            admit_ms,
            serving: AtomicU64::new(0),
            backfilling: AtomicU64::new(0),
            freed: Notify::new(),
            admissions: AtomicU64::new(0),
            busy: AtomicU64::new(0),
            backfill_reads: AtomicU64::new(0),
        }
    }

    pub(crate) const fn capacity(&self) -> u32 {
        self.capacity
    }

    pub(crate) fn admitted_so_far(&self) -> Admitted {
        Admitted {
            admissions: self.admissions.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
            backfill_reads: self.backfill_reads.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn admitted<T, E: From<StoreError>>(
        &self,
        read: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        if sim::mutated(Mutation::AdmissionUnbounded) {
            return read.await;
        }
        self.serving.fetch_add(1, Ordering::SeqCst);
        let demand = Demand(self);
        let deadline = nanocode_platform::sleep(Duration::from_millis(self.admit_ms));
        let permit = match self.reads.acquire_within(deadline).await {
            Ok(permit) => permit,
            Err(AcquireError::Busy) => {
                self.busy.fetch_add(1, Ordering::Relaxed);
                return Err(E::from(StoreError::Unadmitted {
                    capacity: self.capacity,
                    admit_ms: self.admit_ms,
                }));
            }
            Err(AcquireError::Closed) => panic!("the read admission is never closed"),
        };
        self.admissions.fetch_add(1, Ordering::Relaxed);
        let answer = read.await;
        drop(permit);
        drop(demand);
        answer
    }

    fn lane(&self) -> Option<Lane<'_>> {
        let serving = self.serving.load(Ordering::SeqCst);
        let backfilling = self.backfilling.load(Ordering::SeqCst);
        if serving + backfilling >= u64::from(self.capacity) {
            return None;
        }
        let permit = self.reads.try_acquire().ok()?;
        self.backfilling.fetch_add(1, Ordering::SeqCst);
        Some(Lane {
            admission: self,
            permit: Some(permit),
        })
    }

    pub(crate) async fn backfilled<T, E: From<StoreError>>(
        &self,
        read: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        if sim::mutated(Mutation::AdmissionUnbounded)
            || sim::mutated(Mutation::BackfillTakesServingLanes)
            || sim::mutated(Mutation::AdoptInRequest)
        {
            return read.await;
        }
        let lane = loop {
            let freed = self.freed.notified();
            let mut freed = std::pin::pin!(freed);
            freed.as_mut().enable();
            if let Some(lane) = self.lane() {
                break lane;
            }
            freed.await;
        };
        assert!(
            self.backfilling.load(Ordering::SeqCst) <= u64::from(self.capacity),
            "backfill reads never pass the read capacity"
        );
        self.backfill_reads.fetch_add(1, Ordering::Relaxed);
        let answer = read.await;
        drop(lane);
        answer
    }
}
