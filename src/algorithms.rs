use std::cmp::Ordering;
use std::fmt::Debug;
use std::time::Duration;
use crate::convex_hull::Point;
use crate::workload::Workload;

pub type ByteSize = u64;
/// Defines compression algorithms
pub trait Algorithm: Debug {
    /// Estimates the compressed size obtained by running this algorithm on workload w.
    fn compressed_size(&self, w: &Workload) -> ByteSize;
    /// Estimates the time budget required to execute this algorithm on workload w.
    fn time_required(&self, w: &Workload) -> Duration;
    /// Runs the compression algorithm on some workload.
    fn execute(&self, w: &Workload);
}

// Specifies metrics related to a specific algorithm ran on a specific workload.
#[derive(Debug)]
pub struct AlgorithmMetrics {
    pub compressed_size: ByteSize,
    pub time_required: Duration,
    pub algorithm: Box<dyn Algorithm>
}

impl AlgorithmMetrics {
    pub fn new(algorithm: Box<dyn Algorithm>, workload: &Workload) -> AlgorithmMetrics {
        AlgorithmMetrics {
            compressed_size: algorithm.compressed_size(workload),
            time_required: algorithm.time_required(workload),
            algorithm
        }
    }
}

impl PartialOrd for AlgorithmMetrics {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.time_required == other.time_required {
            // Secondary index, inverse (smaller is better)
            return other.compressed_size.partial_cmp(&self.compressed_size);
        }
        self.time_required.partial_cmp(&other.time_required)
    }
}

impl Ord for AlgorithmMetrics {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq for AlgorithmMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.compressed_size == other.compressed_size && self.time_required == other.time_required
    }
}

impl Eq for AlgorithmMetrics {}

impl Point for AlgorithmMetrics {
    fn x(&self) -> f64 {
        self.compressed_size as f64
    }

    fn y(&self) -> f64 {
        self.time_required.as_secs_f64()
    }
}