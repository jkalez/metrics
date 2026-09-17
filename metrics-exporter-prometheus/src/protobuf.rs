//! Protobuf serialization support for Prometheus metrics.

use metrics_util::storage::Histogram;
use prost::Message;
use std::io::Write;

use crate::common::{LabelSet, Snapshot};
use crate::distribution::Distribution;
use crate::formatting::sanitize_metric_name;
use crate::native_histogram::NativeHistogram;
use crate::recorder::DescriptionReadHandle;

// Include the generated protobuf code
mod pb {
    #![allow(missing_docs, clippy::trivially_copy_pass_by_ref, clippy::doc_markdown)]
    include!(concat!(env!("OUT_DIR"), "/io.prometheus.client.rs"));
}

#[cfg(feature = "http-listener")]
pub(crate) const PROTOBUF_CONTENT_TYPE: &str =
    "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited";

/// Renders a snapshot of metrics into protobuf format using length-delimited encoding.
///
/// This function takes a snapshot of metrics and converts them into the Prometheus
/// protobuf wire format, where each `MetricFamily` message is prefixed with a varint
/// length header.
#[allow(clippy::too_many_lines)]
pub(crate) fn render_protobuf(
    snapshot: Snapshot,
    descriptions_rd: &DescriptionReadHandle,
    counter_suffix: Option<&'static str>,
) -> Vec<u8> {
    let mut output = Vec::new();
    render_protobuf_to_write(&mut output, snapshot, descriptions_rd, counter_suffix)
        .expect("writing to an in-memory buffer should not fail");
    output
}

/// Renders a snapshot of metrics into protobuf format using length-delimited encoding.
///
/// This function takes a snapshot of metrics and converts them into the Prometheus
/// protobuf wire format, where each `MetricFamily` message is prefixed with a varint
/// length header.
#[allow(clippy::too_many_lines)]
pub(crate) fn render_protobuf_to_write<W: Write>(
    writer: &mut W,
    snapshot: Snapshot,
    descriptions_rd: &DescriptionReadHandle,
    counter_suffix: Option<&'static str>,
) -> std::io::Result<()> {
    let mut buffer = Vec::new();

    // Process counters
    for (name, by_labels) in snapshot.counters {
        let sanitized_name = sanitize_metric_name(&name);
        let help = descriptions_rd
            .get_one(name.as_str())
            .map(|entry| {
                let (desc, _) = &*entry;
                desc.to_string()
            })
            .unwrap_or_default();

        let mut metrics = Vec::new();
        for (labels, value) in by_labels {
            let label_pairs = label_set_to_protobuf(labels);

            metrics.push(pb::Metric {
                label: label_pairs,
                counter: Some(pb::Counter {
                    #[allow(clippy::cast_precision_loss)]
                    value: Some(value as f64),

                    ..Default::default()
                }),

                ..Default::default()
            });
        }

        let metric_family = pb::MetricFamily {
            name: Some(add_suffix_to_name(&sanitized_name, counter_suffix)),
            help: if help.is_empty() { None } else { Some(help) },
            r#type: Some(pb::MetricType::Counter as i32),
            metric: metrics,
            unit: None,
        };

        buffer.clear();
        metric_family.encode_length_delimited(&mut buffer).unwrap();
        writer.write_all(&buffer)?;
    }

    // Process gauges
    for (name, by_labels) in snapshot.gauges {
        let sanitized_name = sanitize_metric_name(&name);
        let help = descriptions_rd
            .get_one(name.as_str())
            .map(|entry| {
                let (desc, _) = &*entry;
                desc.to_string()
            })
            .unwrap_or_default();

        let mut metrics = Vec::new();
        for (labels, value) in by_labels {
            let label_pairs = label_set_to_protobuf(labels);

            metrics.push(pb::Metric {
                label: label_pairs,
                gauge: Some(pb::Gauge { value: Some(value) }),

                ..Default::default()
            });
        }

        let metric_family = pb::MetricFamily {
            name: Some(sanitized_name),
            help: if help.is_empty() { None } else { Some(help) },
            r#type: Some(pb::MetricType::Gauge as i32),
            metric: metrics,
            unit: None,
        };

        buffer.clear();
        metric_family.encode_length_delimited(&mut buffer).unwrap();
        writer.write_all(&buffer)?;
    }

    // Process distributions (histograms and summaries)
    for (name, by_labels) in snapshot.distributions {
        let sanitized_name = sanitize_metric_name(&name);
        let help = descriptions_rd
            .get_one(name.as_str())
            .map(|entry| {
                let (desc, _) = &*entry;
                desc.to_string()
            })
            .unwrap_or_default();

        let mut metrics = Vec::new();
        let mut metric_type = None;
        for (labels, distribution) in by_labels {
            let label_pairs = label_set_to_protobuf(labels);

            let metric = match distribution {
                Distribution::Summary(summary, quantiles, sum) => {
                    use quanta::Instant;
                    metric_type = Some(pb::MetricType::Summary);
                    let snapshot = summary.snapshot(Instant::now());
                    let quantile_values: Vec<pb::Quantile> = quantiles
                        .iter()
                        .map(|q| pb::Quantile {
                            quantile: Some(q.value()),
                            value: Some(snapshot.quantile(q.value()).unwrap_or(0.0)),
                        })
                        .collect();

                    pb::Metric {
                        label: label_pairs,
                        summary: Some(pb::Summary {
                            sample_count: Some(summary.count() as u64),
                            sample_sum: Some(sum),
                            quantile: quantile_values,

                            created_timestamp: None,
                        }),

                        ..Default::default()
                    }
                }
                Distribution::Histogram(histogram) => {
                    metric_type = Some(pb::MetricType::Histogram);
                    pb::Metric {
                        label: label_pairs,
                        histogram: Some(pb::Histogram::from(&histogram)),
                        ..Default::default()
                    }
                }
                Distribution::Both(classic, native) => {
                    metric_type = Some(pb::MetricType::Histogram);
                    let mut histogram = pb::Histogram::from(&native);
                    histogram.bucket = pb::Histogram::from(&classic).bucket;
                    pb::Metric {
                        label: label_pairs,
                        histogram: Some(histogram),
                        ..Default::default()
                    }
                }
                Distribution::NativeHistogram(native_hist) => {
                    metric_type = Some(pb::MetricType::Histogram);
                    pb::Metric {
                        label: label_pairs,
                        histogram: Some(pb::Histogram::from(&native_hist)),
                        ..Default::default()
                    }
                }
            };

            metrics.push(metric);
        }

        let Some(metric_type) = metric_type else {
            // Skip empty metric families
            continue;
        };

        let metric_family = pb::MetricFamily {
            name: Some(sanitized_name),
            help: if help.is_empty() { None } else { Some(help) },
            r#type: Some(metric_type as i32),
            metric: metrics,
            unit: None,
        };

        buffer.clear();
        metric_family.encode_length_delimited(&mut buffer).unwrap();
        writer.write_all(&buffer)?;
    }

    Ok(())
}

impl From<&Histogram> for pb::Histogram {
    fn from(histogram: &Histogram) -> Self {
        let mut buckets = Vec::new();
        for (le, count) in histogram.buckets() {
            buckets.push(pb::Bucket {
                cumulative_count: Some(count),
                upper_bound: Some(le),
                ..Default::default()
            });
        }
        buckets.push(pb::Bucket {
            cumulative_count: Some(histogram.count()),
            upper_bound: Some(f64::INFINITY),
            ..Default::default()
        });
        Self {
            sample_count: Some(histogram.count()),
            sample_sum: Some(histogram.sum()),
            bucket: buckets,
            ..Default::default()
        }
    }
}

impl From<&NativeHistogram> for pb::Histogram {
    fn from(native_hist: &NativeHistogram) -> Self {
        let (positive_spans, positive_deltas) = make_buckets(&native_hist.positive_buckets());
        let (negative_spans, negative_deltas) = make_buckets(&native_hist.negative_buckets());
        let mut histogram = Self {
            sample_count: Some(native_hist.count()),
            sample_sum: Some(native_hist.sum()),
            zero_threshold: Some(native_hist.config().zero_threshold()),
            schema: Some(native_hist.schema()),
            zero_count: Some(native_hist.zero_count()),
            positive_span: positive_spans,
            positive_delta: positive_deltas,
            negative_span: negative_spans,
            negative_delta: negative_deltas,
            ..Default::default()
        };
        // An empty span distinguishes an empty native histogram from a classic histogram.
        if histogram.zero_threshold == Some(0.0)
            && histogram.zero_count == Some(0)
            && histogram.positive_span.is_empty()
            && histogram.negative_span.is_empty()
        {
            histogram.positive_span = vec![pb::BucketSpan { offset: Some(0), length: Some(0) }];
        }
        histogram
    }
}

fn label_set_to_protobuf(labels: LabelSet) -> Vec<pb::LabelPair> {
    let mut label_pairs = Vec::new();

    for (key, value) in labels.labels {
        label_pairs.push(pb::LabelPair { name: Some(key), value: Some(value) });
    }

    label_pairs
}

fn add_suffix_to_name(name: &str, suffix: Option<&'static str>) -> String {
    match suffix {
        Some(suffix) if !name.ends_with(suffix) => format!("{name}_{suffix}"),
        _ => name.to_string(),
    }
}

/// Convert a `BTreeMap` of bucket indices to counts into Prometheus native histogram
/// spans and deltas format. This follows the Go `makeBucketsFromMap` function.
fn make_buckets(buckets: &std::collections::BTreeMap<i32, u64>) -> (Vec<pb::BucketSpan>, Vec<i64>) {
    if buckets.is_empty() {
        return (vec![], vec![]);
    }

    // Get sorted bucket indices (similar to Go's sorting)
    let mut indices: Vec<i32> = buckets.keys().copied().collect();
    indices.sort_unstable();

    let mut spans = Vec::new();
    let mut deltas = Vec::new();
    let mut prev_count = 0i64;
    let mut next_i = 0i32;

    for (n, &i) in indices.iter().enumerate() {
        #[allow(clippy::cast_possible_wrap)]
        let count = buckets[&i] as i64;

        // Multiple spans with only small gaps in between are probably
        // encoded more efficiently as one larger span with a few empty buckets.
        // Following Go: gaps of one or two buckets should not create a new span.
        let i_delta = i - next_i;

        if n == 0 || i_delta > 2 {
            // Create a new span - either first bucket or gap > 2
            spans.push(pb::BucketSpan { offset: Some(i_delta), length: Some(0) });
        } else {
            // Small gap (or no gap) - insert empty buckets as needed
            for _ in 0..i_delta {
                if let Some(last_span) = spans.last_mut() {
                    *last_span.length.as_mut().unwrap() += 1;
                }
                deltas.push(-prev_count);
                prev_count = 0;
            }
        }

        // Add the current bucket
        if let Some(last_span) = spans.last_mut() {
            *last_span.length.as_mut().unwrap() += 1;
        }
        deltas.push(count - prev_count);
        prev_count = count;
        next_i = i + 1;
    }

    (spans, deltas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Snapshot;
    use crate::recorder::new_description_handles;
    use crate::PrometheusBuilder;
    use indexmap::IndexMap;
    use metrics::{
        ExponentialHistogramSnapshot, HistogramBuckets, HistogramSnapshot, SharedString,
    };
    use prost::Message;
    use std::collections::HashMap;

    #[test]
    fn test_configured_histogram_records_both_representations() {
        use crate::{Matcher, NativeHistogramConfig};

        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(Matcher::Full("latency".into()), &[1.0])
            .unwrap()
            .set_native_histogram_for_metric(
                Matcher::Prefix(String::new()),
                NativeHistogramConfig::new(1.1, 160, 0.0).unwrap(),
            )
            .build_recorder();
        metrics::with_local_recorder(&recorder, || {
            for value in [0.5, 2.0] {
                metrics::histogram!("latency").record(value);
            }
        });
        let handle = recorder.handle();
        let bytes = handle.render_protobuf();
        let family = pb::MetricFamily::decode_length_delimited(&bytes[..]).unwrap();
        let histogram = family.metric[0].histogram.as_ref().unwrap();
        assert_eq!(histogram.sample_count, Some(2));
        assert_eq!(histogram.bucket[0].cumulative_count, Some(1));
        assert!(histogram.schema.is_some());
        assert!(!histogram.positive_span.is_empty());
        assert!(handle.render().contains("latency_bucket{le=\"1\"} 1"));
    }

    #[test]
    fn test_imported_histogram_preserves_both_representations() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let histogram = metrics::with_local_recorder(&recorder, || metrics::histogram!("remote"));
        let snapshot = HistogramSnapshot {
            count: 6,
            sum: 2.0,
            buckets: HistogramBuckets::Both {
                classic: vec![(-1.0, 2), (0.0, 3), (2.0, 6)],
                exponential: ExponentialHistogramSnapshot {
                    scale: 1,
                    zero_threshold: 0.125,
                    zero_count: 1,
                    positive: [(1, 3)].into(),
                    negative: [(0, 2)].into(),
                },
            },
        };
        histogram.set_snapshot(&snapshot);
        let bytes = handle.render_protobuf();
        let family = pb::MetricFamily::decode_length_delimited(&bytes[..]).unwrap();
        assert_eq!(family.r#type, Some(pb::MetricType::Histogram as i32));
        let actual = family.metric[0].histogram.as_ref().unwrap();
        assert_eq!(actual.sample_count, Some(6));
        assert_eq!(actual.sample_sum, Some(2.0));
        assert_eq!(
            actual.bucket.iter().map(|b| (b.upper_bound, b.cumulative_count)).collect::<Vec<_>>(),
            vec![
                (Some(-1.0), Some(2)),
                (Some(0.0), Some(3)),
                (Some(2.0), Some(6)),
                (Some(f64::INFINITY), Some(6))
            ]
        );
        assert_eq!(actual.schema, Some(1));
        assert_eq!(actual.zero_count, Some(1));
        assert_eq!(actual.zero_threshold, Some(0.125));
        assert_eq!(actual.positive_span, vec![pb::BucketSpan { offset: Some(1), length: Some(1) }]);
        assert_eq!(actual.positive_delta, vec![3]);
        assert_eq!(actual.negative_span, vec![pb::BucketSpan { offset: Some(0), length: Some(1) }]);
        assert_eq!(actual.negative_delta, vec![2]);
    }

    #[test]
    fn test_render_protobuf_counters() {
        let mut counters = HashMap::new();
        let mut counter_labels = HashMap::new();
        let labels = LabelSet::from_key_and_global(
            &metrics::Key::from_parts("", vec![metrics::Label::new("method", "GET")]),
            &IndexMap::new(),
        );
        counter_labels.insert(labels, 42u64);
        counters.insert("http_requests".to_string(), counter_labels);

        let snapshot = Snapshot { counters, gauges: HashMap::new(), distributions: HashMap::new() };

        let (mut descriptions_wr, descriptions_rd) = new_description_handles();
        descriptions_wr.publish();

        let protobuf_data = render_protobuf(snapshot, &descriptions_rd, Some("total"));

        assert!(!protobuf_data.is_empty(), "Protobuf data should not be empty");

        // Parse the protobuf response to verify it's correct
        let metric_family = pb::MetricFamily::decode_length_delimited(&protobuf_data[..]).unwrap();

        assert_eq!(metric_family.name.as_ref().unwrap(), "http_requests_total");
        assert_eq!(metric_family.r#type.unwrap(), pb::MetricType::Counter as i32);
        assert_eq!(metric_family.metric.len(), 1);

        let metric = &metric_family.metric[0];
        assert!(metric.counter.is_some());
        let counter_value = metric.counter.as_ref().unwrap().value.unwrap();
        assert!((counter_value - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_render_protobuf_gauges() {
        let mut gauges = HashMap::new();
        let mut gauge_labels = HashMap::new();
        let labels = LabelSet::from_key_and_global(
            &metrics::Key::from_parts("", vec![metrics::Label::new("instance", "localhost")]),
            &IndexMap::new(),
        );
        gauge_labels.insert(labels, 0.75f64);
        gauges.insert("cpu_usage".to_string(), gauge_labels);

        let snapshot = Snapshot { counters: HashMap::new(), gauges, distributions: HashMap::new() };

        let (mut descriptions_wr, descriptions_rd) = new_description_handles();
        descriptions_wr.update(
            "cpu_usage".to_string(),
            (SharedString::const_str("CPU usage percentage"), None),
        );
        descriptions_wr.publish();

        let protobuf_data = render_protobuf(snapshot, &descriptions_rd, None);

        assert!(!protobuf_data.is_empty(), "Protobuf data should not be empty");

        // Parse the protobuf response to verify it's correct
        let metric_family = pb::MetricFamily::decode_length_delimited(&protobuf_data[..]).unwrap();

        assert_eq!(metric_family.name.as_ref().unwrap(), "cpu_usage");
        assert_eq!(metric_family.r#type.unwrap(), pb::MetricType::Gauge as i32);
        assert_eq!(metric_family.help.as_ref().unwrap(), "CPU usage percentage");

        let metric = &metric_family.metric[0];
        assert!(metric.gauge.is_some());
        let gauge_value = metric.gauge.as_ref().unwrap().value.unwrap();
        assert!((gauge_value - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_add_suffix_to_name() {
        assert_eq!(add_suffix_to_name("requests", Some("total")), "requests_total");
        assert_eq!(add_suffix_to_name("requests_total", Some("total")), "requests_total");
        assert_eq!(add_suffix_to_name("requests", None), "requests");
    }
}
