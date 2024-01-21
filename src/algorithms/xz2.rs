use std::io::{Cursor, Write};
use std::time::{Duration, Instant};
use xz2::write::XzEncoder;
use crate::algorithms::{Algorithm, ByteSize};
use crate::workload::Workload;

#[derive(Debug)]
pub struct Xz2CompressionLevel(pub u32);
#[derive(Debug)]
pub struct Xz2 {
    compression_level: Xz2CompressionLevel,
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>
}

impl Xz2 {
    pub fn new(compression_level: Xz2CompressionLevel) -> Xz2 {
        Xz2 {
            compression_level,
            compressed_size: None,
            time_required: None
        }
    }

    fn calculate_metrics(&mut self, workload: &Workload) {
        log::debug!("Calculating compressed size and time required for algorithm {:?}", self);
        let current_unix = Instant::now();
        let compressed_data = self.execute(workload);
        let time = current_unix.elapsed();
        log::debug!("Compressed size and time required calculated for algorithm {:?}:\nCompressed size: {:?};\nTime required: {:?}", self, compressed_data.len() as ByteSize, time);
        self.compressed_size = Some(compressed_data.len() as ByteSize);
        self.time_required = Some(time);
    }
}
impl Algorithm for Xz2 {
    fn name(&self) -> String {
        format!("LZMA {}", self.compression_level.0)
    }
    fn compressed_size(&mut self, w: &Workload) -> ByteSize {
        self.compressed_size.unwrap_or_else(|| {
            self.calculate_metrics(w);
            self.compressed_size.unwrap()
        })
    }

    fn time_required(&mut self, w: &Workload) -> Duration {
        self.time_required.unwrap_or_else(|| {
            self.calculate_metrics(w);
            self.time_required.unwrap()
        })
    }

    fn execute(&self, w: &Workload) -> Vec<u8> {
        let mut e = XzEncoder::new(Vec::with_capacity(w.data.len()), self.compression_level.0);
        e.write_all(w.data).unwrap();
        let res = e.finish().unwrap();
        res
    }

    fn execute_with_target(&self, w: &Workload, target: &mut Cursor<Vec<u8>>) {
        let instant = Instant::now();
        log::debug!("Execute with target: init {:?}", instant.elapsed());
        let mut e = XzEncoder::new(target, self.compression_level.0);
        log::debug!("Execute with target: encoder created {:?}", instant.elapsed());
        e.write_all(w.data).unwrap();
        log::debug!("Execute with target: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute with target: finished {:?}", instant.elapsed());
    }
}