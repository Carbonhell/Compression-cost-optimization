use std::error::Error;
use std::fmt;
use std::fs::File;
use std::str::FromStr;
use std::time::Duration;
use clap::{CommandFactory, Parser};
use clap::error::ErrorKind;
use mix_compression::{process_multiple_documents, process_single_document};
use mix_compression::algorithms::{Algorithm, EstimateMetadata};
use mix_compression::algorithms::bzip2::{Bzip2, Bzip2CompressionLevel};
use mix_compression::algorithms::gzip::{Gzip, GzipCompressionLevel};
use mix_compression::algorithms::xz2::{Xz2, Xz2CompressionLevel};
use mix_compression::workload::Workload;

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
    #[arg(short, long, required(true), value_delimiter = ',', value_parser = parse_key_val::< String, Alg >)]
    documents: Vec<(String, Alg)>,

    /// Time budget, represented as a f64 value describing the budget in seconds.
    #[arg(short, long)]
    budget: f64,

    /// Estimate metrics calculation by using a portion of the workload instead of executing a full run. Requires specifying --estimate-block-ratio and --estimate-block-number flags. Avoid using estimation for small workloads (e.g. workloads requiring less than a 100 seconds budget)
    #[arg(short, long)]
    estimate: bool,

    /// The fraction of the workload to use for algorithm metrics estimation (between 0. and 1.). Bigger means better estimates but slower execution.
    #[arg(short, long, value_parser = parse_ratio)]
    estimate_block_ratio: Option<f64>,

    /// The number of blocks to use to estimate the algorithm metrics. More blocks generate a better averaged estimate, but the execution is slower.
    #[arg(short, long)]
    estimate_block_number: Option<u64>,
}

#[derive(Debug)]
struct AlgParseError(String);

impl std::fmt::Display for AlgParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not parse algorithm: {}", self.0)
    }
}

impl Error for AlgParseError {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Alg {
    Gzip,
    Bzip2,
    Xz2,
}

impl FromStr for Alg {
    type Err = AlgParseError;

    fn from_str(input: &str) -> Result<Alg, Self::Err> {
        match input {
            "gzip" => Ok(Alg::Gzip),
            "bzip2" => Ok(Alg::Bzip2),
            "xz2" => Ok(Alg::Xz2),
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
        }
    }
}

fn main() {
    env_logger::init();
    let args = Cli::parse();
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

    if args.documents.len() == 1 {
        let (file_name, alg) = args.documents.first().unwrap();
        let mut algorithms: Vec<Box<dyn Algorithm>> = Vec::with_capacity(9);

        let mut workload = Workload::new(format!("{}_{}", alg, file_name),
                                         File::open(format!("data/{}", file_name))
                                         .expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.")
                                         , Duration::from_secs_f64(args.budget));

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
        }
        log::info!("Applying mixed compression to single file '{}'", file_name);
        process_single_document(workload, algorithms);
    } else {
        let mut workloads = Vec::new();
        let mut workload_algorithms = Vec::new();
        for (workload_filename, alg) in args.documents {
            let mut algorithms: Vec<Box<dyn Algorithm>> = Vec::with_capacity(9);
            let mut workload = Workload::new(format!("{}_{}", alg, workload_filename),
                                             File::open(format!("data/{}", workload_filename))
                                                 .expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.")
                                             , Duration::from_secs(0));
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
            }
            workloads.push(workload);
            workload_algorithms.push(algorithms);
        }
        log::info!(
            "Applying mixed compression to multiple documents: {:?}, with duration: {}s",
            workloads.iter().map(|el| el.name.clone()).collect::<Vec<_>>(),
            args.budget);
        process_multiple_documents(workloads, workload_algorithms, Duration::from_secs_f64(args.budget))
    }
}

