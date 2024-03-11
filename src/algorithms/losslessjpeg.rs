use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use felics::compression::{ColorType, CompressDecompress, CompressedImage, PixelDepth};
use image::{DynamicImage, GenericImageView, ImageDecoder, ImageEncoder, Rgba};
use image::codecs::png::{PngDecoder, PngEncoder};
pub use image::codecs::png::CompressionType as PNGCompressionType;
pub use image::codecs::png::FilterType as PNGFilterType;
use rand::Rng;
use tempfile::tempfile;
use zune_core::bit_depth::BitDepth;
use zune_core::colorspace::ColorSpace;
use zune_core::options::EncoderOptions;
use zune_jpegxl::JxlSimpleEncoder;

use crate::algorithms::{Algorithm, BlockInfo, ByteSize, EstimateMetadata};
use crate::workload::{FolderWorkload, Workload};

// The following implementation is only useful for time and size calculations. Whereas the byte payload is correctly calculated, there is no support for the header required for a decodeable Lossless JPEG encoded file. Even without the header, this implementation should be good enough to evaluate usefulness in mixed setups.
#[derive(Debug)]
pub struct LosslessJPEG {
    compressed_size: Option<ByteSize>,
    time_required: Option<Duration>,
    predictor: u32,
}

impl LosslessJPEG {
    pub fn new(predictor: u32) -> LosslessJPEG {
        LosslessJPEG {
            compressed_size: None,
            time_required: None,
            predictor,
        }
    }
    pub fn new_folder_workload(workload: &mut FolderWorkload, predictor: u32, estimate_metadata: Option<EstimateMetadata>) -> LosslessJPEG {
        let mut losslessjpeg = LosslessJPEG {
            compressed_size: None,
            time_required: None,
            predictor,
        };
        losslessjpeg.calculate_metrics_folder(workload, estimate_metadata);
        losslessjpeg
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

    fn huffman_table(value: i16) -> u16 {
        match value {
            0 => 0,
            -1 | 1 => 1,
            -3 | -2 | 2 | 3 => 2,
            -7..=-4 | 4..=7 => 3,
            -15..=-8 | 8..=15 => 4,
            -31..=-16 | 16..=31 => 5,
            -63..=-32 | 32..=63 => 6,
            -127..=-64 | 64..=127 => 7,
            -255..=-128 | 128..=255 => 8,
            -511..=-256 | 256..=511 => 9,
            -1023..=-512 | 512..=1023 => 10,
            -2047..=-1024 | 1024..=2047 => 11,
            -4095..=-2048 | 2048..=4095 => 12,
            -8191..=-4096 | 4096..=8191 => 13,
            -16383..=-8192 | 8192..=16383 => 14,
            -32767..=-16384 | 16384..=32767 => 15,
            //32768 => 16,
            _ => panic!("Cannot encode difference with Huffman coding")
        }
    }
}

impl Algorithm for LosslessJPEG {
    fn name(&self) -> String {
        "LosslessJPEG".to_string()
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

        // https://www.w3.org/Graphics/JPEG/itu-t81.pdf
        let mut result = Vec::new();
        let image_width = image.width();
        let empty_pixel = Rgba::from([0u16, 0, 0, 0]);
        let precision = 16; // fixed precision of bits per sample

        for (x, y, pixel) in image.pixels() {
            let pixel_a = if x > 0 { result.get((y * image_width + x - 1) as usize).unwrap_or(&empty_pixel) } else {&empty_pixel};
            let pixel_b = if y > 0 {result.get(((y - 1) * image_width + x) as usize).unwrap_or(&empty_pixel) } else {&empty_pixel};
            let pixel_c = if x > 0 && y > 0 { result.get(((y - 1) * image_width + x - 1) as usize).unwrap_or(&empty_pixel) } else {&empty_pixel};

            let predicted_pixel = if x == 0 && y == 0 {
                Rgba::from([2 ^ (precision - 1), 2 ^ (precision - 1), 2 ^ (precision - 1), 2 ^ (precision - 1)]) // "At the beginning of the first line and at the beginning of each restart interval the prediction value of 2P – 1 is used, where P is the input precision"
            } else if result.len() < image_width as usize {
                pixel_a.clone() // "The one-dimensional horizontal predictor (prediction sample Ra) is used for the first line of samples at the start of the scan"
            } else if x == 0 {
                pixel_b.clone() // "The sample from the line above (prediction sample Rb) is used at the start of each line, except for the first line."
            } else {
                match self.predictor {
                    0 => Rgba::from([0, 0, 0, 0]),
                    1 => pixel_a.clone(),
                    2 => pixel_b.clone(),
                    3 => pixel_c.clone(),
                    4 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            rgba[x] += pixel_a.0[x];
                            rgba[x] += pixel_b.0[x];
                            rgba[x] -= pixel_c.0[x];
                        }
                        Rgba::from(rgba)
                    }
                    5 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            let b_minus_c = pixel_b.0[x] - pixel_c.0[x];
                            rgba[x] += pixel_a.0[x];
                            rgba[x] += b_minus_c >> 1;
                        }
                        Rgba::from(rgba)
                    }
                    6 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            let a_minus_c = pixel_a.0[x] - pixel_c.0[x];
                            rgba[x] += pixel_b.0[x];
                            rgba[x] += a_minus_c >> 1;
                        }
                        Rgba::from(rgba)
                    }
                    7 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            let a_plus_b = pixel_a.0[x] + pixel_b.0[x];
                            rgba[x] += a_plus_b >> 1;
                        }
                        Rgba::from(rgba)
                    }
                    _ => panic!("Unknown predictor used for Lossless JPEG encoding.")
                }
            };

            let result_pixel = {
                let mut rgba = [0u16; 4];
                for x in 0..4 {
                    let pred = predicted_pixel.0[x] as i16;
                    let curr = pixel.0[x] as i16;
                    let diff = (pred - curr) % (2 ^ precision as i16); // "The difference between the prediction value and the input is calculated modulo 2 16 ."
                    rgba[x] = LosslessJPEG::huffman_table(diff);
                }
                Rgba::from(rgba)
            };

            result.push(result_pixel);
        }

        // SOI markers
        w.result_file.write(&[0xFF, 0xD8]).unwrap();
        let pixels = result.iter().map(|el| el.0).flatten().map(|el| el.to_be_bytes()).flatten().collect::<Vec<_>>();
        w.result_file.write(&pixels).unwrap();
        // EOI markers
        w.result_file.write(&[0xFF, 0xD9]).unwrap();

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

        // https://www.w3.org/Graphics/JPEG/itu-t81.pdf
        let mut result = Vec::new();
        let image_width = image.width();
        let empty_pixel = Rgba::from([0u16, 0, 0, 0]);
        let precision = 16; // fixed precision of bits per sample

        for (x, y, pixel) in image.pixels() {
            let pixel_a = if x > 0 { result.get((y * image_width + x - 1) as usize).unwrap_or(&empty_pixel) } else {&empty_pixel};
            let pixel_b = if y > 0 {result.get(((y - 1) * image_width + x) as usize).unwrap_or(&empty_pixel) } else {&empty_pixel};
            let pixel_c = if x > 0 && y > 0 { result.get(((y - 1) * image_width + x - 1) as usize).unwrap_or(&empty_pixel) } else {&empty_pixel};

            let predicted_pixel = if x == 0 && y == 0 {
                Rgba::from([2 ^ (precision - 1), 2 ^ (precision - 1), 2 ^ (precision - 1), 2 ^ (precision - 1)]) // "At the beginning of the first line and at the beginning of each restart interval the prediction value of 2P – 1 is used, where P is the input precision"
            } else if result.len() < image_width as usize {
                pixel_a.clone() // "The one-dimensional horizontal predictor (prediction sample Ra) is used for the first line of samples at the start of the scan"
            } else if x == 0 {
                pixel_b.clone() // "The sample from the line above (prediction sample Rb) is used at the start of each line, except for the first line."
            } else {
                match self.predictor {
                    0 => Rgba::from([0, 0, 0, 0]),
                    1 => pixel_a.clone(),
                    2 => pixel_b.clone(),
                    3 => pixel_c.clone(),
                    4 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            rgba[x] += pixel_a.0[x];
                            rgba[x] += pixel_b.0[x];
                            rgba[x] -= pixel_c.0[x];
                        }
                        Rgba::from(rgba)
                    }
                    5 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            let b_minus_c = pixel_b.0[x] - pixel_c.0[x];
                            rgba[x] += pixel_a.0[x];
                            rgba[x] += b_minus_c >> 1;
                        }
                        Rgba::from(rgba)
                    }
                    6 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            let a_minus_c = pixel_a.0[x] - pixel_c.0[x];
                            rgba[x] += pixel_b.0[x];
                            rgba[x] += a_minus_c >> 1;
                        }
                        Rgba::from(rgba)
                    }
                    7 => {
                        let mut rgba = [0; 4];
                        for x in 0..4 {
                            let a_plus_b = pixel_a.0[x] + pixel_b.0[x];
                            rgba[x] += a_plus_b >> 1;
                        }
                        Rgba::from(rgba)
                    }
                    _ => panic!("Unknown predictor used for Lossless JPEG encoding.")
                }
            };

            let result_pixel = {
                let mut rgba = [0u16; 4];
                for x in 0..4 {
                    let pred = predicted_pixel.0[x] as i16;
                    let curr = pixel.0[x] as i16;
                    let diff = (pred - curr) % (2 ^ precision as i16); // "The difference between the prediction value and the input is calculated modulo 2 16 ."
                    rgba[x] = LosslessJPEG::huffman_table(diff);
                }
                Rgba::from(rgba)
            };

            result.push(result_pixel);
        }

        // SOI markers
        tmpfile.write(&[0xFF, 0xD8]).unwrap();
        let bytes = result.iter().map(|el| el.0).flatten().map(|el| el.to_be_bytes()).flatten().collect::<Vec<_>>();
        tmpfile.write(&bytes).unwrap();
        // EOI markers
        tmpfile.write(&[0xFF, 0xD9]).unwrap();

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
                Some(File::create(Path::new("results").join(&w.name).join(direntry.file_name())).unwrap()),
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

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::time::Duration;

    use crate::algorithms::Algorithm;
    use crate::algorithms::losslessjpeg::LosslessJPEG;
    use crate::workload::Workload;

    #[test]
    fn create_jpeg() {
        let encoder = LosslessJPEG::new(7);
        encoder.execute(&mut Workload::new("test_lossless".to_string(), File::open("data/PNG_Test.png").unwrap(), Duration::from_secs(0), None));
    }

    #[test]
    fn read_jpeg() {
        let mut x = BufReader::new(File::open("results/test_lossless.zip").unwrap());
        let mut buf = [0];
        x.read_exact(&mut buf);
        println!("{:?}", buf);
        let mut decoder = jpeg_decoder::Decoder::new(BufReader::new(File::open("results/test_lossless.zip").unwrap()));
        let pixels = decoder.decode().expect("failed to decode image");
        let metadata = decoder.info().unwrap();
    }
}