use std::sync::Arc;

use arc_swap::ArcSwap;
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
#[derive(Debug)]
pub struct HistogramHandle {
    state: ArcSwap<HistogramState>,
}

#[derive(Debug)]
enum HistogramState {
    Observations(AtomicBucketInstant<f64>),
    Snapshot(Option<HistogramSnapshot>),
}

impl HistogramHandle {
    fn new() -> Self {
        Self {
            state: ArcSwap::from_pointee(HistogramState::Observations(AtomicBucketInstant::new())),
        }
    }

    pub fn drain_into(&self, distribution: &mut Distribution) {
        match &**self.state.load() {
            HistogramState::Observations(observations) => {
                observations.clear_with(|samples| distribution.record_samples(samples));
            }
            HistogramState::Snapshot(Some(_)) => {
                // The transition is one-way, so swapping cannot discard an observation buffer.
                // Taking the latest state atomically leaves any later publication pending.
                let previous = self.state.swap(Arc::new(HistogramState::Snapshot(None)));
                if let HistogramState::Snapshot(Some(snapshot)) = &*previous {
                    *distribution = snapshot.clone().into();
                }
            }
            HistogramState::Snapshot(None) => {}
        }
    }
}

impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        if let HistogramState::Observations(observations) = &**self.state.load() {
            observations.record(value);
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
        self.state.store(Arc::new(HistogramState::Snapshot(Some(snapshot.clone()))));
    }
}

#[cfg(test)]
mod tests {
    use super::{HistogramHandle, HistogramState};
    use crate::distribution::Distribution;
    use metrics::{HistogramBuckets, HistogramFn, HistogramSnapshot};

    #[test]
    fn test_in_flight_observation_cannot_modify_imported_snapshot() {
        let handler = HistogramHandle::new();
        // Keep the state loaded by a record call that began before the mode switch.
        let in_flight = handler.state.load();
        let HistogramState::Observations(observations) = &**in_flight else {
            panic!("expected observation mode");
        };
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
