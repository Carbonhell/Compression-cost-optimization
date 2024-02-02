pub mod gzip;
pub mod bzip2;
pub mod xz2;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::fs::File;
use std::time::Duration;
use crate::convex_hull::Point;
use crate::workload::Workload;

pub type ByteSize = u64;

/// Defines compression algorithms
pub trait Algorithm: Debug {
    fn name(&self) -> String;
    /// Estimates the compressed size obtained by running this algorithm on workload w.
    fn compressed_size(&self) -> ByteSize;
    /// Estimates the time budget required to execute this algorithm on workload w.
    fn time_required(&self) -> Duration;
    /// Runs the compression algorithm on some workload.
    fn execute(&self, w: &mut Workload);
    fn execute_on_tmp(&self, w: &mut Workload, block_info: Option<BlockInfo>) -> File;

    /// Runs the compression algorithm on some workload, by writing on a cursor target to optimize memory writes.
    fn execute_with_target(&self, w: &mut Workload, partition: usize, first_half: bool);
}


// Specifies metrics related to a specific algorithm ran on a specific workload.
#[derive(Debug)]
pub struct AlgorithmMetrics {
    pub compressed_size: ByteSize,
    pub time_required: Duration,
    pub algorithm: Box<dyn Algorithm>,
}

impl AlgorithmMetrics {
    pub fn new(algorithm: Box<dyn Algorithm>) -> AlgorithmMetrics {
        AlgorithmMetrics {
            compressed_size: algorithm.compressed_size(),
            time_required: algorithm.time_required(),
            algorithm,
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
        self.time_required.as_secs_f64()
    }

    fn y(&self) -> f64 {
        self.compressed_size as f64
    }
}

#[derive(Copy, Clone)]
pub struct EstimateMetadata {
    pub block_number: u64,
    pub block_ratio: f64,
}

pub struct BlockInfo {
    pub block_size: u64,
    pub block_end_index: u64,
}