use std::{env, fs};
use std::time::Duration;
use log::debug;
use crate::algorithms::{AlgorithmMetrics};
use crate::algorithms::gzip::{Gzip, GzipCompressionLevel};
use crate::mixing_policy::MixingPolicy;
use crate::workload::Workload;

mod workload;
mod algorithms;
mod mixing_policy;
mod convex_hull;

fn main() {
    env_logger::init();
    let file_name = env::args().nth(1).expect("No filename given");
    let time_budget = env::args().nth(2).expect("No time budget (number of seconds) given").parse::<f64>().expect("Expected number of seconds");
    println!("Applying mixed compression to file '{}'", file_name);
    single_document(file_name.as_str(), time_budget);
}

fn single_document(filename: &str, time_budget: f64) {
    let pagelinks = fs::read(format!("data/{}", filename)).expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.");
    let workload = Workload::new(&pagelinks, Duration::from_secs_f64(time_budget));
    debug!("Workload size: {:?}, time budget: {:?}", workload.data.len(), workload.time_budget);
    let mut algorithms = Vec::with_capacity(9);
    for i in 1..=9 {
        algorithms.push(Gzip::new(GzipCompressionLevel(i)))
    }
    let algorithms: Vec<_> = algorithms
        .into_iter()
        .map(|alg| {
            AlgorithmMetrics::new(Box::new(alg), &workload)
        })
        .collect();
    let mixing_policy = MixingPolicy::new(algorithms.iter().collect());
    let optimal_mix = mixing_policy.optimal_mix(&workload);
    match optimal_mix {
        Some(optimal_mix) => {
            let compressed_workload = MixingPolicy::apply_optimal_mix(optimal_mix, &workload);
            fs::write(format!("results/{}.zip", filename), compressed_workload).expect(format!("Couldn't write to path 'results/{}.zip'", filename).as_str());
        },
        None => {
            let minimum_time_budget = mixing_policy
                .lower_convex_hull
                .iter()
                .map(|el| el.0)
                .min();
            match minimum_time_budget {
                Some(min) => {
                    println!("No algorithm found that can compress data in the given time budget (Budget is {:?}, cheapest algorithm requires {:?}).", workload.time_budget, min.time_required)
                },
                None => {
                    println!("The polygonal chain is empty. Is this an error?");
                }
            }
        }
    }
}
