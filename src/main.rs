use std::error::Error;
use std::fmt;
use std::fs::{File, metadata};
use std::io::{Read, Seek, SeekFrom};
use std::str::FromStr;
use std::time::Duration;
use clap::{CommandFactory, Parser};
use clap::error::ErrorKind;
use mix_compression::{algorithms, process_folder, process_multiple_documents, process_single_document};
use mix_compression::algorithms::{Algorithm, EstimateMetadata};
use mix_compression::algorithms::bzip2::{Bzip2, Bzip2CompressionLevel};
use mix_compression::algorithms::gzip::{Gzip, GzipCompressionLevel};
use mix_compression::algorithms::xz2::{Xz2, Xz2CompressionLevel};
use mix_compression::workload::{FolderWorkload, Workload};
#[cfg(feature = "image")]
use {
    mix_compression::algorithms::png::{PNG, PNGCompressionType, PNGFilterType},
    image::codecs::png::{PngDecoder, PngEncoder},
    image::{ImageDecoder, ImageEncoder},
    crate::Alg::FELICS
};

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
    where
        T: FromStr,
        T::Err: Error + Send + Sync + 'static,
        U: FromStr,
        U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

/// Parse a string argument into a f64, ensuring it exists within a 0..=1 range
fn parse_ratio(s: &str) -> Result<f64, Box<dyn Error + Send + Sync + 'static>> {
    let float = s.parse::<f64>().map_err(|_| format!("invalid f64 argument: {s} (cannot parse)"))?;
    if float < 0. || float > 1. {
        Err(format!("invalid f64 argument: {s} (out of 0..=1 range)"))?;
    }
    Ok(float)
}

/// A general optimization framework to allocate computing resources to the compression of massive and heterogeneous data sets.
///
/// Specify which documents to compress (from the `data` folder) and the time budget to allocate for the compression.
/// The program will output the compressed results in the `results` folder, along with useful plots showing the lower convex hulls and benefits of the mixing strategies.
///
/// If a single document is passed, it will be compressed by taking the optimal mix of all levels of the provided algorithm to satisfy the time budget constraint.
/// If multiple documents are passed, the time budget constraint will be applied to the whole compression task. In this case, one document will possibly benefit of a level mixing strategy, while the others will be compressed with a specific algorithm level.
/// The mixing strategy works by mixing compression settings (the level) for a specific algorithm.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// List of file names from the `data` folder to process, associated to the algorithm to use and separated with a comma.
    /// Algorithms currently supported: gzip, bzip2, xz2.
    ///
    /// For example: `RLbook2020.pdf=gzip,cyber.pdf=bzip2` will set up a mix job using gzip for `RLbook2020.pdf` and bzip2 for `cyber.pdf`. Documents can be repeated as long as they use different algorithms, e.g. `cyber.pdf=gzip,cyber.pdf=xz2`.
    #[arg(short, long, value_delimiter = ',', value_parser = parse_key_val::< String, Alg >)]
    documents: Vec<(String, Alg)>,

    /// Time budget, represented as a f64 value describing the budget in seconds.
    #[arg(short, long)]
    budget: Option<f64>,

    /// Estimate metrics calculation by using a portion of the workload instead of executing a full run. Requires specifying --estimate-block-ratio and --estimate-block-number flags. Avoid using estimation for small workloads (e.g. workloads requiring less than a 100 seconds budget)
    #[arg(short, long)]
    estimate: bool,

    /// The fraction of the workload to use for algorithm metrics estimation (between 0. and 1.). Bigger means better estimates but slower execution.
    #[arg(short = 'r', long, value_parser = parse_ratio)]
    estimate_block_ratio: Option<f64>,

    /// The number of blocks to use to estimate the algorithm metrics. More blocks generate a better averaged estimate, but the execution is slower.
    #[arg(short = 'n', long)]
    estimate_block_number: Option<u64>,

    #[arg(long)]
    decompress: Option<String>,
}

#[derive(Debug)]
struct AlgParseError(String);

impl std::fmt::Display for AlgParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not parse algorithm \"{}\". Ensure you have the correct feature flags enabled.", self.0)
    }
}

impl Error for AlgParseError {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Alg {
    Gzip,
    Bzip2,
    Xz2,
    Png,
    FELICS,
    JPEGXL,
    Lossless,
}

impl FromStr for Alg {
    type Err = AlgParseError;

    fn from_str(input: &str) -> Result<Alg, Self::Err> {
        match input {
            "gzip" => Ok(Alg::Gzip),
            "bzip2" => Ok(Alg::Bzip2),
            "xz2" => Ok(Alg::Xz2),
            #[cfg(feature = "image")]
            "png" => Ok(Alg::Png),
            "felics" => Ok(Alg::FELICS),
            "jpegxl" => Ok(Alg::JPEGXL),
            "lossless" => Ok(Alg::Lossless),
            _ => Err(AlgParseError(String::from(input))),
        }
    }
}

impl fmt::Display for Alg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Alg::Gzip => write!(f, "gzip"),
            Alg::Bzip2 => write!(f, "bzip2"),
            Alg::Xz2 => write!(f, "xz2"),
            Alg::Png => write!(f, "png"),
            Alg::FELICS => write!(f, "felics"),
            Alg::JPEGXL => write!(f, "jpegxl"),
            Alg::Lossless => write!(f, "lossless"),
        }
    }
}

fn main() {
    env_logger::init();
    let args = Cli::parse();
    if let Some(decompress_file) = args.decompress {
        #[cfg(feature = "image")]
        {
            let mut file = File::open(format!("results/{}.zip", decompress_file)).unwrap();
            let mut header_buffer = [0; 11];
            file.read(&mut header_buffer).unwrap();
            log::debug!("Header: {:?} - {}", header_buffer, file.metadata().unwrap().len());
            if header_buffer == [137u8, 77u8, 73u8, 88u8, 80u8, 78u8, 71u8, 13u8, 10u8, 26u8, 10u8] {
                log::debug!("Matches!");
                let mut partition = [0; 8]; // an u64
                let mut original_width = [0; 4]; // u32
                let mut original_height = [0; 4]; // u32
                file.read(&mut partition).unwrap();
                file.read(&mut original_width).unwrap();
                file.read(&mut original_height).unwrap();
                let partition = u64::from_be_bytes(partition);
                let original_width = u32::from_be_bytes(original_width);
                let original_height = u32::from_be_bytes(original_height);
                log::debug!("Partition is {}, width and height: {}-{}", partition, original_width, original_height);

                let (first_image_bytes, color_type) = {
                    let png = PngDecoder::new(&file).unwrap();
                    let color_type = png.color_type();
                    log::debug!("First image metadata: {}, {}, {}, {:?}", png.total_bytes(), png.dimensions().0, png.dimensions().1, color_type);
                    let mut image_bytes = vec![0; png.total_bytes() as usize];
                    png.read_image(&mut image_bytes).expect("failed to read first image");
                    (image_bytes, color_type)
                };

                log::debug!("{} - {}", file.stream_position().unwrap(), file.metadata().unwrap().len());
                file.seek(SeekFrom::Start(partition)).unwrap();
                let mut header_buffer = [0; 11];
                file.read(&mut header_buffer).unwrap();
                log::debug!("Second header: {:?} - {}", header_buffer, file.metadata().unwrap().len());
                if header_buffer == [137u8, 77u8, 73u8, 88u8, 80u8, 78u8, 71u8, 13u8, 10u8, 26u8, 10u8] {
                    log::debug!("Second half matches!");

                    let second_png = PngDecoder::new(&file).unwrap();
                    // seeked
                    log::debug!("Second image metadata: {}, {}, {}", second_png.total_bytes(), second_png.dimensions().0, second_png.dimensions().1);

                    let mut image_bytes = vec![0; second_png.total_bytes() as usize];
                    second_png.read_image(&mut image_bytes).expect("failed to read first image");
                    let mut complete_image_bytes = first_image_bytes;
                    complete_image_bytes.extend_from_slice(&image_bytes);

                    let x = PngEncoder::new(File::create(format!("results/uncompressed_{}", decompress_file)).unwrap());
                    x.write_image(&complete_image_bytes, original_width, original_height, color_type).expect("Failed to write uncompressed png");
                }
            }
            return;
        }

        #[cfg(not(feature = "image"))]
        panic!("Decompressing works only for images and requires the \"image\" feature.");
    }

    let estimate_metadata = if args.estimate {
        if let (Some(block_number), Some(block_ratio)) = (args.estimate_block_number, args.estimate_block_ratio) {
            Some(EstimateMetadata{ block_number, block_ratio })
        } else {
            let mut cmd = Cli::command();
            cmd.error(
                ErrorKind::MissingRequiredArgument,
                "Estimating algorithm metrics requires passing both the --estimate-block-number and --estimate-block-ratio flags.",
            )
                .exit();
        }
    } else {
        None
    };

    if args.documents.is_empty() {
        let mut cmd = Cli::command();
        cmd.error(
            ErrorKind::MissingRequiredArgument,
            "You must pass at least one document.",
        )
            .exit();
    }

    let budget = if let Some(budget) = args.budget {
        budget
    } else {
        let mut cmd = Cli::command();
        cmd.error(
            ErrorKind::MissingRequiredArgument,
            "The --budget argument was not provided.",
        )
            .exit();
    };

    if args.documents.len() == 1 {
        let (file_name, alg) = args.documents.first().unwrap();
        let mut algorithms: Vec<Box<dyn Algorithm>> = Vec::new();

        if metadata(format!("data/{}", file_name)).unwrap().is_dir() {
            let mut workload = FolderWorkload::new(file_name.clone(), Duration::from_secs_f64(budget));
            match alg {
                Alg::Png => {
                    #[cfg(feature = "image")]
                    for compression_type in vec![PNGCompressionType::Fast, PNGCompressionType::Best] {
                        for filter_type in vec![
                            PNGFilterType::NoFilter,
                            PNGFilterType::Adaptive,
                            PNGFilterType::Avg,
                            PNGFilterType::Paeth,
                            PNGFilterType::Sub,
                            PNGFilterType::Up
                        ] {
                            algorithms.push(Box::new(PNG::new_folder_workload(&mut workload, compression_type, filter_type, estimate_metadata)))
                        }
                    }
                },
                Alg::FELICS => {
                    #[cfg(feature = "image")]
                    algorithms.push(Box::new(algorithms::felics::FELICS::new_folder_workload(&mut workload, estimate_metadata)))
                },
                Alg::JPEGXL => {
                    #[cfg(feature = "image")]
                    algorithms.push(Box::new(algorithms::jpegxl::JPEGXL::new_folder_workload(&mut workload, estimate_metadata)))
                },
                Alg::Lossless => {
                    #[cfg(feature = "image")]
                    {
                        for compression_type in vec![PNGCompressionType::Fast, PNGCompressionType::Best] {
                            for filter_type in vec![
                                PNGFilterType::NoFilter,
                                PNGFilterType::Adaptive,
                                PNGFilterType::Avg,
                                PNGFilterType::Paeth,
                                PNGFilterType::Sub,
                                PNGFilterType::Up
                            ] {
                                algorithms.push(Box::new(PNG::new_folder_workload(&mut workload, compression_type, filter_type, estimate_metadata)))
                            }
                        }
                        algorithms.push(Box::new(algorithms::felics::FELICS::new_folder_workload(&mut workload, estimate_metadata)));
                        algorithms.push(Box::new(algorithms::jpegxl::JPEGXL::new_folder_workload(&mut workload, estimate_metadata)));
                        algorithms.push(Box::new(algorithms::losslessjpeg::LosslessJPEG::new_folder_workload(&mut workload, 7, estimate_metadata)));
                    }
                }
                _ => {todo!()}
            }
            log::info!("Applying mixed compression to single file '{}'", file_name);
            process_folder(workload, algorithms);
        } else {
        let mut workload = Workload::new(format!("{}_{}", alg, file_name),
                                         File::open(format!("data/{}", file_name))
                                         .expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.")
                                         , Duration::from_secs_f64(budget), None);

        match alg {
            Alg::Gzip => {
                for i in 1..=9 {
                    algorithms.push(Box::new(Gzip::new(&mut workload, GzipCompressionLevel(i), estimate_metadata)))
                }
            }
            Alg::Bzip2 => {
                for i in 1..=9 {
                    algorithms.push(Box::new(Bzip2::new(&mut workload, Bzip2CompressionLevel(i), estimate_metadata)))
                }
            }
            Alg::Xz2 => {
                for i in 1..=9 {
                    algorithms.push(Box::new(Xz2::new(&mut workload, Xz2CompressionLevel(i), estimate_metadata)))
                }
            }
            Alg::Png => {
                #[cfg(feature = "image")]
                for compression_type in vec![PNGCompressionType::Fast, PNGCompressionType::Best] {
                    for filter_type in vec![
                        PNGFilterType::NoFilter,
                        PNGFilterType::Adaptive,
                        PNGFilterType::Avg,
                        PNGFilterType::Paeth,
                        PNGFilterType::Sub,
                        PNGFilterType::Up
                    ] {
                        algorithms.push(Box::new(PNG::new(&mut workload, compression_type, filter_type, estimate_metadata)))
                    }
                }
            }
            _ => panic!("Algorithm not supported on single files.")
        }
        log::info!("Applying mixed compression to single file '{}'", file_name);
        process_single_document(workload, algorithms);
            }
    } else {
        let mut workloads = Vec::new();
        let mut workload_algorithms = Vec::new();
        for (workload_filename, _) in args.documents.iter() {
            if metadata(format!("data/{}", workload_filename)).unwrap().is_dir() {
                panic!("Multiple folder processing is currently not supported.");
            }
        }

        for (workload_filename, alg) in args.documents {
            let mut algorithms: Vec<Box<dyn Algorithm>> = Vec::with_capacity(9);
            let mut workload = Workload::new(format!("{}_{}", alg, workload_filename),
                                             File::open(format!("data/{}", workload_filename))
                                                 .expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.")
                                             , Duration::from_secs(0), None);
            match alg {
                Alg::Gzip => {
                    for i in 1..=9 {
                        algorithms.push(Box::new(Gzip::new(&mut workload, GzipCompressionLevel(i), estimate_metadata)))
                    }
                }
                Alg::Bzip2 => {
                    for i in 1..=9 {
                        algorithms.push(Box::new(Bzip2::new(&mut workload, Bzip2CompressionLevel(i), estimate_metadata)))
                    }
                }
                Alg::Xz2 => {
                    for i in 1..=9 {
                        algorithms.push(Box::new(Xz2::new(&mut workload, Xz2CompressionLevel(i), estimate_metadata)))
                    }
                }
                Alg::Png => {
                    #[cfg(feature = "image")]
                    for compression_type in vec![PNGCompressionType::Fast, PNGCompressionType::Best] {
                        for filter_type in vec![
                            PNGFilterType::NoFilter,
                            PNGFilterType::Adaptive,
                            PNGFilterType::Avg,
                            PNGFilterType::Paeth,
                            PNGFilterType::Sub,
                            PNGFilterType::Up
                        ] {
                            algorithms.push(Box::new(PNG::new(&mut workload, compression_type, filter_type, estimate_metadata)))
                        }
                    }
                }
                _ => panic!("Algorithm not supported on specific files.")
            }
            workloads.push(workload);
            workload_algorithms.push(algorithms);
        }
        log::info!(
            "Applying mixed compression to multiple documents: {:?}, with duration: {}s",
            workloads.iter().map(|el| el.name.clone()).collect::<Vec<_>>(),
            budget);
        process_multiple_documents(workloads, workload_algorithms, Duration::from_secs_f64(budget))
    }
}

