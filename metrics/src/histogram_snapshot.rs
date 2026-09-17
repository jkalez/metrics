use std::collections::BTreeMap;

/// A cumulative histogram aggregate, suitable for re-emitting measurements collected elsewhere.
///
/// Classic and exponential buckets, when both present, must describe the same observations.
/// All series sharing a metric name should use the same representation.
#[derive(Clone, Debug, PartialEq)]
pub struct HistogramSnapshot {
    /// Total number of observations, including those outside the finite classic buckets.
    pub count: u64,
    /// Sum of all observations.
    pub sum: f64,
    /// Bucket representation of the observations.
    pub buckets: HistogramBuckets,
}

/// Bucket representations available in a histogram snapshot.
#[derive(Clone, Debug, PartialEq)]
pub enum HistogramBuckets {
    /// Classic buckets only.
    Classic(ClassicHistogramSnapshot),
    /// Exponential buckets only.
    Exponential(ExponentialHistogramSnapshot),
    /// Both representations of the same observations.
    Both {
        /// Classic upper bounds and cumulative counts.
        classic: ClassicHistogramSnapshot,
        /// Exponential buckets.
        exponential: ExponentialHistogramSnapshot,
    },
}

/// Classic buckets as `(upper_bound, cumulative_count)` pairs.
///
/// Upper bounds must be finite and strictly increasing. Counts include observations equal to
/// the bound, must be nondecreasing, and must not exceed the snapshot's total count.
/// The implicit positive-infinity bucket contains all observations.
pub type ClassicHistogramSnapshot = Vec<(f64, u64)>;

/// Exponential histogram buckets with base `2^(2^-scale)`.
///
/// Positive bucket index `i` represents `(base^(i-1), base^i]`; negative buckets mirror these
/// intervals around zero. This is the Prometheus index convention. OpenTelemetry bucket
/// indices must be incremented by one when importing its exponential histograms.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExponentialHistogramSnapshot {
    /// Resolution of the exponential buckets. Supported resolutions depend on the recorder.
    pub scale: i32,
    /// Nonnegative threshold bounding the inclusive zero bucket on either side of zero.
    pub zero_threshold: f64,
    /// Number of observations in the zero bucket.
    pub zero_count: u64,
    /// Positive bucket indices and noncumulative counts, excluding zero-bucket observations.
    pub positive: BTreeMap<i32, u64>,
    /// Negative bucket indices and noncumulative counts, excluding zero-bucket observations.
    pub negative: BTreeMap<i32, u64>,
}
