use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use image::{GenericImageView, ImageDecoder, ImageEncoder};
use image::codecs::png::{PngDecoder, PngEncoder};
pub use image::codecs::png::CompressionType as PNGCompressionType;
pub use image::codecs::png::FilterType as PNGFilterType;
use rand::Rng;
use tempfile::tempfile;

use crate::algorithms::{Algorithm, BlockInfo, ByteSize, EstimateMetadata};
use crate::workload::{FolderWorkload, Workload};

#[derive(Debug)]
pub struct PNG {
    compression_type: PNGCompressionType,
    filter_type: PNGFilterType,
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>,
}

impl PNG {
    pub fn new(workload: &mut Workload, compression_type: PNGCompressionType, filter_type: PNGFilterType, estimate_metadata: Option<EstimateMetadata>) -> PNG {
        let mut png = PNG {
            compression_type,
            filter_type,
            compressed_size: None,
            time_required: None,
        };
        png.calculate_metrics(workload, estimate_metadata);
        png
    }

    pub fn new_folder_workload(workload: &mut FolderWorkload, compression_type: PNGCompressionType, filter_type: PNGFilterType, estimate_metadata: Option<EstimateMetadata>) -> PNG {
        let mut png = PNG {
            compression_type,
            filter_type,
            compressed_size: None,
            time_required: None,
        };
        png.calculate_metrics_folder(workload, estimate_metadata);
        png
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
                    let block_compressed_size = self.execute_on_tmp(workload, Some(BlockInfo { block_size, block_end_index })).metadata().unwrap().len();
                    let time = current_unix.elapsed().as_secs_f64();
                    average_time_required += time;
                    average_compressed_size += block_compressed_size;
                }
                average_compressed_size = ((average_compressed_size as f64 / metadata.block_number as f64) * (1. / metadata.block_ratio).round()) as u64;
                average_time_required = (average_time_required / metadata.block_number as f64) * (1. / metadata.block_ratio);
                log::debug!("Final metrics:\nCompressed size: {}\nTime required: {}\nTime taken for estimation: {:?}", average_compressed_size, average_time_required, current_unix.elapsed());
                (average_compressed_size, Duration::from_secs_f64(average_time_required))
            }
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

    // in this case EstimateMetadata block_ratio indicates the % of files from the folder to use, and block_number how many repetitions with different files
    fn calculate_metrics_folder(&mut self, workload: &mut FolderWorkload, estimate_metadata: Option<EstimateMetadata>) {
        log::info!("Calculating compressed size and time required for algorithm {:?} (workload \"{}\") (estimating: {})", self, workload.name, estimate_metadata.is_some());
        let (compressed_size, time_required) = match estimate_metadata {
            Some(_) => {
                unimplemented!("Estimating time required and compressed size for folder workloads is currently not supported.")
            }
            None => {
                let current_unix = Instant::now();
                let result = self.execute_on_folder(workload, true, None, false);
                (result, current_unix.elapsed())
            }
        };
        log::info!("Compressed size and time required calculated for algorithm {:?}:\nCompressed size: {:?};\nTime required: {:?}", self, compressed_size as ByteSize, time_required);
        self.compressed_size = Some(compressed_size as ByteSize);
        self.time_required = Some(time_required);
    }
}

impl Algorithm for PNG {
    fn name(&self) -> String {
        format!("PNG_{:?}_{:?}", self.compression_type, self.filter_type)
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

        let e = PngEncoder::new_with_quality(&mut w.result_file, self.compression_type, self.filter_type);
        log::debug!("Execute: encoder created {:?}", instant.elapsed());

        let mut buffer = Vec::new();
        w.data.read_to_end(&mut buffer).unwrap();
        let image = image::load_from_memory(&buffer).unwrap();
        let (dimension_width, dimension_height) = image.dimensions();
        let color_type = image.color();

        e.write_image(image.as_bytes(), dimension_width, dimension_height, color_type)
            .expect("Failed to write png data");
        log::debug!("Execute: finished {:?}", instant.elapsed());

        w.data.rewind().unwrap();
    }

    fn execute_on_tmp(&self, w: &mut Workload, block_info: Option<BlockInfo>) -> File {
        let instant = Instant::now();
        log::debug!("Execute on tmp: init {:?}", instant.elapsed());

        let tmpfile = tempfile().unwrap();
        let e = PngEncoder::new_with_quality(&tmpfile, self.compression_type, self.filter_type);
        log::debug!("Execute on tmp: encoder created {:?}", instant.elapsed());

        let mut buffer = Vec::new();
        w.data.read_to_end(&mut buffer).unwrap();
        let image = image::load_from_memory(&buffer).unwrap();
        let (dimension_width, dimension_height) = image.dimensions();
        let color_type = image.color();
        let bytes_per_pixel = color_type.bytes_per_pixel() as u64;
        let image_total_size = image.as_bytes().len();


        let block_info = block_info.unwrap_or(BlockInfo { block_size: w.data.metadata().unwrap().len(), block_end_index: w.data.metadata().unwrap().len() });
        let block_size = block_info.block_size;
        let fraction = block_size as f64 / w.data.metadata().unwrap().len() as f64;
        let mixed_width = dimension_width;
        let mixed_height = (dimension_height as f64 * fraction).round() as u32;
        let partitioned_total_size = (mixed_width * mixed_height).saturating_mul(bytes_per_pixel as u32);
        let (start, data_len) = if block_info.block_end_index == block_info.block_size {
            (0usize, partitioned_total_size as usize)
        } else {
            ((image_total_size as u64 - partitioned_total_size as u64) as usize, image_total_size as usize)
        };

        e.write_image(&image.as_bytes()[start..data_len], mixed_width, mixed_height, color_type)
            .expect("Failed to write png data");
        log::debug!("Execute on tmp: finished {:?}", instant.elapsed());

        w.data.rewind().unwrap();
        tmpfile
    }

    fn execute_with_target(&self, w: &mut Workload, partition: usize, first_half: bool) {
        let instant = Instant::now();
        log::debug!("Execute with target: init {:?}", instant.elapsed());

        log::debug!("Execute with target: encoder created {:?}", instant.elapsed());

        let decoder = PngDecoder::new(&w.data)
            .expect("Failed to decode workload png data");
        let (original_width, original_height) = decoder.dimensions();
        let color_type = decoder.color_type();
        let bytes_per_pixel = color_type.bytes_per_pixel() as u64;
        let image_total_size = decoder.total_bytes();

        let mut buf: Vec<u8> = vec![0; image_total_size as usize];
        log::debug!("Reading img in buf of {} (usize {}) - original width {}, original height {}, color {}",
            image_total_size, image_total_size as usize, original_width, original_height, bytes_per_pixel);
        decoder.read_image(&mut buf).expect("Failed to read workload png data");
        let mut fraction = partition as f64 / w.data.metadata().unwrap().len() as f64;
        if !first_half {
            fraction = 1. - fraction;
        }
        let mixed_width = original_width;
        let mixed_height = (original_height as f64 * fraction).round() as u32;
        let partitioned_total_size = (mixed_width * mixed_height).saturating_mul(bytes_per_pixel as u32);

        let (pos, data_len, size) = if first_half {
            let end = partitioned_total_size;
            (0usize, end as usize, end)
        } else {
            let start = image_total_size as u32 - partitioned_total_size;
            let end = image_total_size as u32;
            (start as usize, end as usize, end - start)
        };

        log::debug!("Pos: {}, data_len: {}, size: {}", pos, data_len, size);

        // Similar to the png signature http://www.libpng.org/pub/png/spec/1.2/PNG-Rationale.html#R.PNG-file-signature but with "MIXPNG" to denote the mixed nature and metadata to get the original width, height and index of the next image partition
        let mut custom_header = vec![137u8, 77u8, 73u8, 88u8, 80u8, 78u8, 71u8, 13u8, 10u8, 26u8, 10u8];
        if first_half {
            custom_header.extend_from_slice(&[0; 8]); // Prepare some space for a u64 containing the index for the next image
            custom_header.extend_from_slice(&[0; 8]); // and for two u32 for the original width and height
        }
        // Write a custom header which contains the partition index that splits the two png payloads for decoding
        w.result_file.write(custom_header.as_slice()).expect("Couldn't write png");
        let partition_index = w.result_file.stream_position().unwrap() - 16;
        log::debug!("partition index is {}, width: {}, height: {}", partition_index, mixed_width, mixed_height);
        let e = PngEncoder::new_with_quality(&w.result_file, self.compression_type, self.filter_type);
        e.write_image(&buf[pos..data_len], mixed_width, mixed_height, color_type)
            .expect("Failed to write png data");
        let next_image_index = w.result_file.stream_position().unwrap();
        if first_half {
            // Write the index of the start of the next MIXPNG signature
            w.result_file.seek(SeekFrom::Start(partition_index)).unwrap();
            w.result_file.write(&next_image_index.to_be_bytes()).unwrap();
            w.result_file.write(&original_width.to_be_bytes()).unwrap();
            w.result_file.write(&original_height.to_be_bytes()).unwrap();
            w.result_file.seek(SeekFrom::Start(next_image_index)).unwrap();
        }

        log::debug!("Execute with target: finished {:?} - size {}, width {}, pos {}", instant.elapsed(), data_len, mixed_width, next_image_index);
        w.data.rewind().unwrap();
    }

    fn execute_on_folder(&self, w: &mut FolderWorkload, write_to_tmp: bool, max_size: Option<u64>, first_half: bool) -> u64 {
        let mut size = 0;
        // read_dir doesn't guarantee any consistent order - sort files by size
        let mut files = Vec::new();
        for path in w.get_data_folder() {
            files.push(path.unwrap());
        }
        files.sort_by_key(|a| a.metadata().unwrap().len());
        // If partially compressing the folder, partition the directory now
        if let Some(max_size) = max_size {
            let mut actual_files = Vec::new();
            let mut data_size = 0;
            for path in files {
                let len = path.metadata().unwrap().len();
                if data_size < max_size && first_half || data_size > max_size && !first_half {
                    actual_files.push(path);
                }
                data_size += len;
            }
            files = actual_files;
        }

        for direntry in files {
            let mut file_workload = Workload::new(
                format!("{}-{:?}", w.name, direntry.file_name()),
                File::open(direntry.path()).unwrap(),
                w.time_budget,
                Some(File::create(Path::new("results").join(&w.name).join(direntry.file_name())).unwrap())
            );
            let result = if write_to_tmp { self.execute_on_tmp(&mut file_workload, None) } else {
                self.execute(&mut file_workload);
                file_workload.result_file
            };
            size += result.metadata().unwrap().len();
        }
        size
    }
}