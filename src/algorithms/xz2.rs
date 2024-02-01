use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};
use tempfile::tempfile;
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
    pub fn new(workload: &mut Workload, compression_level: Xz2CompressionLevel) -> Xz2 {
        let mut xz = Xz2 {
            compression_level,
            compressed_size: None,
            time_required: None
        };
        xz.calculate_metrics(workload);
        xz
    }

    fn calculate_metrics(&mut self, workload: &mut Workload) {
        log::debug!("Calculating compressed size and time required for algorithm {:?}", self);
        let current_unix = Instant::now();
        let compressed_file = self.execute_on_tmp(workload);
        let time = current_unix.elapsed();
        log::debug!("Compressed size and time required calculated for algorithm {:?}:\nCompressed size: {:?};\nTime required: {:?}", self, compressed_file.metadata().unwrap().len() as ByteSize, time);
        self.compressed_size = Some(compressed_file.metadata().unwrap().len() as ByteSize);
        self.time_required = Some(time);
    }
}
impl Algorithm for Xz2 {
    fn name(&self) -> String {
        format!("LZMA_{}", self.compression_level.0)
    }
    fn compressed_size(&self) -> ByteSize {
        self.compressed_size.unwrap()
    }

    fn time_required(&self) -> Duration {
        self.time_required.unwrap()
    }

    fn execute(&self, w: &mut Workload) {
        let mut e = XzEncoder::new(&mut w.result_file, self.compression_level.0);
        let mut pos = 0usize;
        let data_len = w.data.metadata().unwrap().len() as usize;
        while pos < data_len {
            let buffer_len = min(10_000_000, data_len - pos);
            let mut buffer: Vec<u8> = vec![0; buffer_len];
            w.data.read_exact(&mut buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            pos += buffer_len;
        }
        e.finish().unwrap();
        w.data.rewind().unwrap();
    }

    fn execute_on_tmp(&self, w: &mut Workload) -> File {
        let tmpfile = tempfile().unwrap();
        let mut e = XzEncoder::new(&tmpfile, self.compression_level.0);
        let mut pos = 0usize;
        let data_len = w.data.metadata().unwrap().len() as usize;
        while pos < data_len {
            let buffer_len = min(10_000_000, data_len - pos);
            let mut buffer: Vec<u8> = vec![0; buffer_len];
            w.data.read_exact(&mut buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            pos += buffer_len;
        }
        e.finish().unwrap();
        w.data.rewind().unwrap();
        tmpfile
    }

    fn execute_with_target(&self, w: &mut Workload, partition: usize, first_half: bool) {
        let instant = Instant::now();
        log::debug!("Execute with target: init {:?}", instant.elapsed());
        let mut e = XzEncoder::new(&w.result_file, self.compression_level.0);
        log::debug!("Execute with target: encoder created {:?}", instant.elapsed());
        let (mut pos, data_len) = if first_half {
            (0usize, partition)
        } else {
            (partition, w.data.metadata().unwrap().len() as usize)
        };
        if !first_half {
            w.data.seek(SeekFrom::Start(partition as u64)).expect("Partition is wrong");
        }
        while pos < data_len {
            let buffer_len = min(1_000_000_000, data_len - pos);
            let mut buffer: Vec<u8> = vec![0; buffer_len];
            w.data.read_exact(&mut *buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            pos += buffer_len;
            log::debug!("Execute with target: written {} bytes so far (time: {:?})", pos, instant.elapsed());
        }
        log::debug!("Execute with target: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute with target: finished {:?}", instant.elapsed());
        w.data.rewind().unwrap();
    }
}