use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use felics::compression::{ColorType, CompressDecompress, CompressedImage, PixelDepth};

use image::{DynamicImage, ImageDecoder, ImageEncoder};
use image::codecs::png::{PngDecoder, PngEncoder};
pub use image::codecs::png::CompressionType as PNGCompressionType;
pub use image::codecs::png::FilterType as PNGFilterType;
use rand::Rng;
use tempfile::tempfile;

use crate::algorithms::{Algorithm, BlockInfo, ByteSize, EstimateMetadata};
use crate::workload::{FolderWorkload, Workload};

#[derive(Debug)]
pub struct FELICS {
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>,
}

impl FELICS {
    pub fn new_folder_workload(workload: &mut FolderWorkload, estimate_metadata: Option<EstimateMetadata>) -> FELICS {
        let mut felics = FELICS {
            compressed_size: None,
            time_required: None,
        };
        felics.calculate_metrics_folder(workload, estimate_metadata);
        felics
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

impl Algorithm for FELICS {
    fn name(&self) -> String {
        "FELICS".to_string()
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

        let mut buffer = Vec::new();
        w.data.read_to_end(&mut buffer).unwrap();
        let image = image::load_from_memory(&buffer).unwrap();
        let felics_image = match image {
            DynamicImage::ImageLuma8(image) => {
                image.compress()
            }
            DynamicImage::ImageLuma16(image) => {
                image.compress()
            }
            DynamicImage::ImageRgb8(image) => {
                image.compress()
            }
            DynamicImage::ImageRgb16(image) => {
                image.compress()
            },
            DynamicImage::ImageRgba8(_) => {image.to_rgb8().compress()}
            DynamicImage::ImageRgba16(_) => {image.to_rgb16().compress()}
            DynamicImage::ImageRgb32F(_) => {image.to_rgb16().compress()}
            DynamicImage::ImageRgba32F(_) => {image.to_rgb16().compress()}
            DynamicImage::ImageLumaA8(_) => {image.to_luma8().compress()}
            DynamicImage::ImageLumaA16(_) => {image.to_luma16().compress()}
            _ => {panic!("Source image format not supported by FELICS!")}
        };

        let color_type_code: u8 = match felics_image.color_type {
            ColorType::Gray => 0,
            ColorType::Rgb => 1,
        };

        let pixel_depth_code: u8 = match felics_image.pixel_depth {
            PixelDepth::Eight => 0,
            PixelDepth::Sixteen => 1,
        };

        // the felics library doesn't implement serde
        w.result_file.write(&felics_image.width.to_be_bytes()).unwrap();
        w.result_file.write(&felics_image.height.to_be_bytes()).unwrap();
        w.result_file.write(&color_type_code.to_be_bytes()).unwrap();
        w.result_file.write(&pixel_depth_code.to_be_bytes()).unwrap();

        w.result_file.write(&felics_image.channels.len().to_be_bytes()).unwrap();
        for channel in felics_image.channels {
            w.result_file.write(&channel.pixel1.to_be_bytes()).unwrap();
            w.result_file.write(&channel.pixel2.to_be_bytes()).unwrap();
            w.result_file.write(&channel.data.len().to_be_bytes()).unwrap();
            w.result_file.write(&channel.data.num_bytes().to_be_bytes()).unwrap();
            w.result_file.write(&channel.data.as_raw_bytes()).unwrap();
        }

        log::debug!("Execute: finished {:?}", instant.elapsed());

        w.data.rewind().unwrap();
    }

    fn execute_on_tmp(&self, w: &mut Workload, block_info: Option<BlockInfo>) -> File {
        let instant = Instant::now();
        log::debug!("Execute on tmp: init {:?}", instant.elapsed());

        let mut tmpfile = tempfile().unwrap();
        let mut buffer = Vec::new();
        w.data.read_to_end(&mut buffer).unwrap();
        let image = image::load_from_memory(&buffer).unwrap();
        let felics_image = match image {
            DynamicImage::ImageLuma8(image) => {
                image.compress()
            }
            DynamicImage::ImageLuma16(image) => {
                image.compress()
            }
            DynamicImage::ImageRgb8(image) => {
                image.compress()
            }
            DynamicImage::ImageRgb16(image) => {
                image.compress()
            },
            DynamicImage::ImageRgba8(_) => {image.to_rgb8().compress()}
            DynamicImage::ImageRgba16(_) => {image.to_rgb16().compress()}
            DynamicImage::ImageRgb32F(_) => {image.to_rgb16().compress()}
            DynamicImage::ImageRgba32F(_) => {image.to_rgb16().compress()}
            DynamicImage::ImageLumaA8(_) => {image.to_luma8().compress()}
            DynamicImage::ImageLumaA16(_) => {image.to_luma16().compress()}
            _ => {panic!("Source image format not supported by FELICS!")}
        };

        let color_type_code: u8 = match felics_image.color_type {
            ColorType::Gray => 0,
            ColorType::Rgb => 1,
        };

        let pixel_depth_code: u8 = match felics_image.pixel_depth {
            PixelDepth::Eight => 0,
            PixelDepth::Sixteen => 1,
        };

        // the felics library doesn't implement serde
        tmpfile.write(&felics_image.width.to_be_bytes()).unwrap();
        tmpfile.write(&felics_image.height.to_be_bytes()).unwrap();
        tmpfile.write(&color_type_code.to_be_bytes()).unwrap();
        tmpfile.write(&pixel_depth_code.to_be_bytes()).unwrap();

        tmpfile.write(&felics_image.channels.len().to_be_bytes()).unwrap();
        for channel in felics_image.channels {
            tmpfile.write(&channel.pixel1.to_be_bytes()).unwrap();
            tmpfile.write(&channel.pixel2.to_be_bytes()).unwrap();
            tmpfile.write(&channel.data.len().to_be_bytes()).unwrap();
            tmpfile.write(&channel.data.num_bytes().to_be_bytes()).unwrap();
            tmpfile.write(&channel.data.as_raw_bytes()).unwrap();
        }

        log::debug!("Execute: finished {:?}", instant.elapsed());

        w.data.rewind().unwrap();
        tmpfile
    }

    fn execute_with_target(&self, w: &mut Workload, partition: usize, first_half: bool) {
        unimplemented!()
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