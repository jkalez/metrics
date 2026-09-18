use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_utils::atomic::AtomicCell;
use metrics::{atomics::AtomicU64, HistogramBuckets, HistogramFn, HistogramSnapshot};
use metrics_util::{registry::GenerationalStorage, storage::AtomicBucket};
use quanta::Instant;

use crate::distribution::Distribution;
use crate::native_histogram::{MAX_SCHEMA, MIN_SCHEMA};

pub type GenerationalAtomicStorage = GenerationalStorage<AtomicStorage>;

/// Atomic metric storage for the prometheus exporter.
#[derive(Debug)]
pub struct AtomicStorage;

impl<K> metrics_util::registry::Storage<K> for AtomicStorage {
    type Counter = Arc<AtomicU64>;
    type Gauge = Arc<AtomicU64>;
    type Histogram = Arc<HistogramHandle>;

    fn counter(&self, _: &K) -> Self::Counter {
        Arc::new(AtomicU64::new(0))
    }

    fn gauge(&self, _: &K) -> Self::Gauge {
        Arc::new(AtomicU64::new(0))
    }

    fn histogram(&self, _: &K) -> Self::Histogram {
        Arc::new(HistogramHandle::new())
    }
}

/// An `AtomicBucket` newtype wrapper that tracks the time of value insertion.
#[derive(Debug)]
pub struct AtomicBucketInstant<T> {
    inner: AtomicBucket<(T, Instant)>,
}

impl<T> AtomicBucketInstant<T> {
    fn new() -> AtomicBucketInstant<T> {
        Self { inner: AtomicBucket::new() }
    }

    pub fn clear_with<F>(&self, f: F)
    where
        F: FnMut(&[(T, Instant)]),
    {
        self.inner.clear_with(f);
    }
}

impl HistogramFn for AtomicBucketInstant<f64> {
    fn record(&self, value: f64) {
        let now = Instant::now();
        self.inner.push((value, now));
    }
}

/// A histogram handler that permanently switches to aggregate replacement on its first accepted snapshot.
pub struct HistogramHandle {
    observations: AtomicBucketInstant<f64>,
    snapshot_mode: AtomicBool,
    snapshot: AtomicCell<Option<Box<HistogramSnapshot>>>,
}

impl fmt::Debug for HistogramHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistogramHandle")
            .field("observations", &self.observations)
            .field("snapshot_mode", &self.snapshot_mode)
            .finish_non_exhaustive()
    }
}

impl HistogramHandle {
    fn new() -> Self {
        Self {
            observations: AtomicBucketInstant::new(),
            snapshot_mode: AtomicBool::new(false),
            snapshot: AtomicCell::new(None),
        }
    }

    pub fn drain_into(&self, distribution: &mut Distribution) {
        // The recorder serializes drains, so an observation drain cannot outlive snapshot replacement.
        if self.snapshot_mode.load(Ordering::Acquire) {
            self.observations.clear_with(|_| {});
            if let Some(snapshot) = self.snapshot.take() {
                *distribution = (*snapshot).into();
            }
        } else {
            self.observations.clear_with(|samples| distribution.record_samples(samples));
        }
    }
}

impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        if !self.snapshot_mode.load(Ordering::Acquire) {
            self.observations.record(value);
        }
    }

    /// Ignores snapshots with an unsupported exponential scale.
    /// Changing the scale alone would reinterpret bucket boundaries without rebucketing counts.
    fn set_snapshot(&self, snapshot: &HistogramSnapshot) {
        if let HistogramBuckets::Exponential(exponential) = &snapshot.buckets {
            if !(MIN_SCHEMA..=MAX_SCHEMA).contains(&exponential.scale) {
                return;
            }
        }
        let snapshot = Box::new(snapshot.clone());
        // Disable observation drains before the snapshot can be consumed.
        self.snapshot_mode.store(true, Ordering::Release);
        self.snapshot.store(Some(snapshot));
    }
}

#[cfg(test)]
mod tests {
    use super::HistogramHandle;
    use crate::distribution::Distribution;
    use crossbeam_utils::atomic::AtomicCell;
    use metrics::{HistogramBuckets, HistogramFn, HistogramSnapshot};

    #[test]
    fn test_snapshot_cell_is_lock_free() {
        assert!(AtomicCell::<Option<Box<HistogramSnapshot>>>::is_lock_free());
    }

    #[test]
    fn test_in_flight_observation_cannot_modify_imported_snapshot() {
        let handler = HistogramHandle::new();
        // Simulate a record call that passed the mode check before snapshot publication.
        let observations = &handler.observations;
        handler.set_snapshot(&HistogramSnapshot {
            count: 1,
            sum: 1.0,
            buckets: HistogramBuckets::Classic(vec![(2.0, 1)]),
        });
        let mut distribution = Distribution::new_histogram(&[2.0]);
        handler.drain_into(&mut distribution);
        observations.record(99.0);
        handler.drain_into(&mut distribution);

        let Distribution::Histogram(histogram) = distribution else {
            panic!("expected classic histogram");
        };
        assert_eq!(histogram.count(), 1);
        assert_eq!(histogram.sum().to_bits(), 1.0_f64.to_bits());
    }
}
