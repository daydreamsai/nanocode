use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use nanocode_durability_core::StoreError;
use nanocode_sync::{AcquireError, Semaphore};

use crate::sim::{self, Mutation};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Admitted {
    pub(crate) admissions: u64,
    pub(crate) busy: u64,
}

pub(crate) struct Admission {
    reads: Semaphore,
    capacity: u32,
    admit_ms: u64,
    admissions: AtomicU64,
    busy: AtomicU64,
}

impl Admission {
    pub(crate) fn new(capacity: u32, admit_ms: u64) -> Self {
        assert!(
            admit_ms > 0,
            "a read waits for admission a while before Busy"
        );
        let permits = usize::try_from(capacity).expect("a read capacity fits in usize");
        Self {
            reads: Semaphore::new(permits),
            capacity,
            admit_ms,
            admissions: AtomicU64::new(0),
            busy: AtomicU64::new(0),
        }
    }

    pub(crate) const fn capacity(&self) -> u32 {
        self.capacity
    }

    pub(crate) fn admitted_so_far(&self) -> Admitted {
        Admitted {
            admissions: self.admissions.load(Ordering::Relaxed),
            busy: self.busy.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn admitted<T, E: From<StoreError>>(
        &self,
        read: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        if sim::mutated(Mutation::AdmissionUnbounded) {
            return read.await;
        }
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
        answer
    }
}
