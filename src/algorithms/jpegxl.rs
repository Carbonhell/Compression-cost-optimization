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
use zune_core::bit_depth::BitDepth;
use zune_core::colorspace::ColorSpace;
use zune_jpegxl::JxlSimpleEncoder;
use zune_core::options::EncoderOptions;

use crate::algorithms::{Algorithm, BlockInfo, ByteSize, EstimateMetadata};
use crate::workload::{FolderWorkload, Workload};

#[derive(Debug)]
pub struct JPEGXL {
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>,
}

impl JPEGXL {
    pub fn new_folder_workload(workload: &mut FolderWorkload, estimate_metadata: Option<EstimateMetadata>) -> JPEGXL {
        let mut jpegxl = JPEGXL {
            compressed_size: None,
            time_required: None,
        };
        jpegxl.calculate_metrics_folder(workload, estimate_metadata);
        jpegxl
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

impl Algorithm for JPEGXL {
    fn name(&self) -> String {
        "JPEGXL".to_string()
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

        let (color_space, bit_depth) = match image.color() {
            image::ColorType::L8 => {(ColorSpace::Luma, BitDepth::Eight)}
            image::ColorType::La8 => {(ColorSpace::LumaA, BitDepth::Eight)}
            image::ColorType::Rgb8 => {(ColorSpace::RGB, BitDepth::Eight)}
            image::ColorType::Rgba8 => {(ColorSpace::RGBA, BitDepth::Eight)}
            image::ColorType::L16 => {(ColorSpace::Luma, BitDepth::Sixteen)}
            image::ColorType::La16 => {(ColorSpace::LumaA, BitDepth::Sixteen)}
            image::ColorType::Rgb16 => {(ColorSpace::RGB, BitDepth::Sixteen)}
            image::ColorType::Rgba16 => {(ColorSpace::RGBA, BitDepth::Sixteen)}
            image::ColorType::Rgb32F => {(ColorSpace::RGB, BitDepth::Float32)}
            image::ColorType::Rgba32F => {(ColorSpace::RGBA, BitDepth::Float32)}
            _ => {panic!("Unknown color type!")}
        };
        let mut encoder = JxlSimpleEncoder::new(image.as_bytes(), EncoderOptions::new(image.width() as usize, image.height() as usize, color_space, bit_depth));
        let result = encoder.encode().unwrap();


        w.result_file.write(&result).unwrap();

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

        let (color_space, bit_depth) = match image.color() {
            image::ColorType::L8 => {(ColorSpace::Luma, BitDepth::Eight)}
            image::ColorType::La8 => {(ColorSpace::LumaA, BitDepth::Eight)}
            image::ColorType::Rgb8 => {(ColorSpace::RGB, BitDepth::Eight)}
            image::ColorType::Rgba8 => {(ColorSpace::RGBA, BitDepth::Eight)}
            image::ColorType::L16 => {(ColorSpace::Luma, BitDepth::Sixteen)}
            image::ColorType::La16 => {(ColorSpace::LumaA, BitDepth::Sixteen)}
            image::ColorType::Rgb16 => {(ColorSpace::RGB, BitDepth::Sixteen)}
            image::ColorType::Rgba16 => {(ColorSpace::RGBA, BitDepth::Sixteen)}
            image::ColorType::Rgb32F => {(ColorSpace::RGB, BitDepth::Float32)}
            image::ColorType::Rgba32F => {(ColorSpace::RGBA, BitDepth::Float32)}
            _ => {panic!("Unknown color type!")}
        };
        let mut encoder = JxlSimpleEncoder::new(image.as_bytes(), EncoderOptions::new(image.width() as usize, image.height() as usize, color_space, bit_depth));
        let result = encoder.encode().unwrap();
        tmpfile.write(&result).unwrap();

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