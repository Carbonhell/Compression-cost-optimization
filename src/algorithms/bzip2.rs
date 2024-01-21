use std::io::{Cursor, Write};
use std::time::{Duration, Instant};
use bzip2::Compression;
use bzip2::write::BzEncoder;
use crate::algorithms::{Algorithm, ByteSize};
use crate::workload::Workload;

#[derive(Debug)]
pub struct Bzip2CompressionLevel(pub u32);
#[derive(Debug)]
pub struct Bzip2 {
    compression_level: Bzip2CompressionLevel,
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>
}

impl Bzip2 {
    pub fn new(compression_level: Bzip2CompressionLevel) -> Bzip2 {
        Bzip2 {
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
impl Algorithm for Bzip2 {
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
        let mut e = BzEncoder::new(Vec::with_capacity(w.data.len()), Compression::new(self.compression_level.0));
        e.write_all(w.data).unwrap();
        let res = e.finish().unwrap();
        res
    }

    fn execute_with_target(&self, w: &Workload, target: &mut Cursor<Vec<u8>>) {
        let instant = Instant::now();
        log::debug!("Execute with target: init {:?}", instant.elapsed());
        let mut e = BzEncoder::new(target, Compression::new(self.compression_level.0));
        log::debug!("Execute with target: encoder created {:?}", instant.elapsed());
        e.write_all(w.data).unwrap();
        log::debug!("Execute with target: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute with target: finished {:?}", instant.elapsed());
    }
}