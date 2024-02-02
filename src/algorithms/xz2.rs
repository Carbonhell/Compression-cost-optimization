use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};
use rand::Rng;
use tempfile::tempfile;
use xz2::write::XzEncoder;
use crate::algorithms::{Algorithm, BlockInfo, ByteSize, EstimateMetadata};
use crate::workload::Workload;

#[derive(Debug)]
pub struct Xz2CompressionLevel(pub u32);

#[derive(Debug)]
pub struct Xz2 {
    compression_level: Xz2CompressionLevel,
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>,
}

impl Xz2 {
    pub fn new(workload: &mut Workload, compression_level: Xz2CompressionLevel, estimate_metadata: Option<EstimateMetadata>) -> Xz2 {
        let mut xz = Xz2 {
            compression_level,
            compressed_size: None,
            time_required: None,
        };
        xz.calculate_metrics(workload, estimate_metadata);
        xz
    }

    fn calculate_metrics(&mut self, workload: &mut Workload, estimate_metadata: Option<EstimateMetadata>) {
        log::info!("Calculating compressed size and time required for algorithm {:?} (workload \"{}\") (estimating: {})", self, workload.name, estimate_metadata.is_some());
        let (compressed_size, time_required) = match estimate_metadata {
            Some(metadata) => {
                let mut average_compressed_size = 0;
                let mut average_time_required = 0.;
                let current_unix = Instant::now();
                log::debug!("Estimating metrics by using {} blocks of ratio {}", metadata.block_number, metadata.block_ratio);
                for _ in 0..metadata.block_number {
                    let workload_size = workload.data.metadata().unwrap().len();
                    let block_size = (workload_size as f64 * metadata.block_ratio).round() as u64;
                    let block_end_index = rand::thread_rng().gen_range(block_size..workload_size);
                    let current_unix = Instant::now();
                    let block_compressed_size = self.execute_on_tmp(workload, Some(BlockInfo{ block_size, block_end_index })).metadata().unwrap().len();
                    let time = current_unix.elapsed().as_secs_f64();
                    average_time_required += time;
                    average_compressed_size += block_compressed_size;
                }
                average_compressed_size = ((average_compressed_size as f64 / metadata.block_number as f64) * (1./metadata.block_ratio).round()) as u64;
                average_time_required = (average_time_required / metadata.block_number as f64) * (1./metadata.block_ratio);
                log::debug!("Final metrics:\nCompressed size: {}\nTime required: {}\nTime taken for estimation: {:?}", average_compressed_size, average_time_required, current_unix.elapsed());
                (average_compressed_size, Duration::from_secs_f64(average_time_required))
            },
            None => {
                let current_unix = Instant::now();
                let result = self.execute_on_tmp(workload, None).metadata().unwrap().len();
                (result, current_unix.elapsed())
            }
        };
        log::info!("Compressed size and time required calculated for algorithm {:?}:\nCompressed size: {:?};\nTime required: {:?}", self, compressed_size as ByteSize, time_required);
        self.compressed_size = Some(compressed_size as ByteSize);
        self.time_required = Some(time_required);
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
        let instant = Instant::now();
        log::debug!("Execute: init {:?}", instant.elapsed());
        let mut e = XzEncoder::new(&mut w.result_file, self.compression_level.0);
        log::debug!("Execute: encoder created {:?}", instant.elapsed());
        let mut pos = 0usize;
        let data_len = w.data.metadata().unwrap().len() as usize;
        while pos < data_len {
            let buffer_len = min(10_000_000, data_len - pos);
            let mut buffer: Vec<u8> = vec![0; buffer_len];
            w.data.read_exact(&mut buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            pos += buffer_len;
            log::debug!("Execute: written {} bytes so far (time: {:?})", pos, instant.elapsed());
        }
        log::debug!("Execute: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute: finished {:?}", instant.elapsed());
        w.data.rewind().unwrap();
    }

    fn execute_on_tmp(&self, w: &mut Workload, block_info: Option<BlockInfo>) -> File {
        let instant = Instant::now();
        log::debug!("Execute on tmp: init {:?}", instant.elapsed());
        let tmpfile = tempfile().unwrap();
        let mut e = XzEncoder::new(&tmpfile, self.compression_level.0);
        log::debug!("Execute on tmp: encoder created {:?}", instant.elapsed());
        let block_info = block_info.unwrap_or(BlockInfo{block_size: w.data.metadata().unwrap().len(), block_end_index: w.data.metadata().unwrap().len()});
        let mut start = block_info.block_end_index - block_info.block_size;
        let data_len = block_info.block_end_index;
        while start < data_len {
            let buffer_len = min(10_000_000, data_len - start);
            let mut buffer: Vec<u8> = vec![0; buffer_len as usize];
            w.data.read_exact(&mut buffer).expect(&*format!("Something went wrong while compressing data for workload \"{}\"", w.name));
            e.write_all(&*buffer).expect(&*format!("Something went wrong while writing compressed data for workload \"{}\"", w.name));
            start += buffer_len;
            log::debug!("Execute on tmp: written {} bytes so far (time: {:?})", start, instant.elapsed());
        }
        log::debug!("Execute on tmp: write_all done {:?}", instant.elapsed());
        e.finish().unwrap();
        log::debug!("Execute on tmp: finished {:?}", instant.elapsed());
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