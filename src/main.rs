use std::{env, fs};
use std::process::exit;
use std::time::Duration;
use log::debug;
use plotly::{Bar, Plot, Scatter};
use crate::algorithms::{AlgorithmMetrics};
use crate::algorithms::gzip::{Gzip, GzipCompressionLevel};
use crate::mixing_policy::{MixingPolicy, MixingPolicyMultipleWorkloads};
use crate::workload::Workload;

mod workload;
mod algorithms;
mod mixing_policy;
mod convex_hull;

fn main() {
    env_logger::init();
    let mut args = env::args();
    if args.len() < 2 {
        println!("You must pass at least one filename + time budget (seconds, f64) pair, or a total time budget and a list of file names.");
        exit(1);
    } else if args.len() == 2 {
        let file_name = args.nth(1).expect("No filename given");
        let time_budget = args.nth(2).expect("No time budget (number of seconds) given").parse::<f64>().expect("Expected number of seconds");
        println!("Applying mixed compression to single file '{}'", file_name);
        single_document(file_name.as_str(), time_budget);
    } else {
        let _ = args.next(); // remove the program name
        let workload_duration = args
            .next()
            .unwrap()
            .parse::<f64>()
            .expect("First argument must be a time budget in f64 seconds");
        let mut workload_filenames = Vec::new();
        while let Some(x) = args.next()  {
            println!("Pushing {:?}", x);
            workload_filenames.push(x);
        }
        log::info!("Applying mixed compression to multiple documents: {:?}, with duration: {}s", workload_filenames, workload_duration);
        multiple_documents(workload_filenames, Duration::from_secs_f64(workload_duration))
    }
}

fn single_document(filename: &str, time_budget: f64) {
    let workload_data = fs::read(format!("data/{}", filename)).expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.");
    let workload = Workload::new(&workload_data, Duration::from_secs_f64(time_budget));
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
            let compressed_workload = MixingPolicy::apply_optimal_mix(&optimal_mix, &workload);
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

fn multiple_documents(workload_filenames: Vec<String>, total_time_budget: Duration) {
    let workload_data: Vec<_> = workload_filenames
        .iter()
        .map(|filename| {
            let data: Vec<u8> = fs::read(format!("data/{}", filename)).expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.");
            data
        })
        .collect();
    let workloads: Vec<_> = workload_data
        .iter().map(|data| {
            Workload::new(&data, Duration::from_secs_f64(0.)) // Workload duration is unused since we have a total time budget
    })
        .collect();

    let mut algorithms = Vec::new();
    workloads
        .iter()
        .for_each(|workload| {
            let mut compression_configurations = Vec::with_capacity(9);
            for i in 1..=9 {
                compression_configurations.push(Gzip::new(GzipCompressionLevel(i)))
            }
            let compression_configurations: Vec<_> = compression_configurations
                .into_iter()
                .map(|alg| {
                    AlgorithmMetrics::new(Box::new(alg), &workload)
                })
                .collect();
            algorithms.push(compression_configurations);
        });
    // TODO sort out the borrow issue with &AlgorithmMetrics to remove this hack
    let alg2 = algorithms.iter().map(|el| el.iter().collect()).collect();
    let mixing_policy = MixingPolicyMultipleWorkloads::new(alg2);

    for (metrics, workload_filename) in mixing_policy
        .lower_convex_hull_per_workload
        .iter()
        .zip(&workload_filenames) {
        let mut lch_info = format!("Metrics for workload '{}ì (time - compressed size)", workload_filename);
        for metric in metrics {
            lch_info.push_str(&*format!("\n{} ; {} (benefit {})", metric.0.time_required.as_secs_f32(), metric.0.compressed_size, metric.1));
        }
        log::info!("{}", lch_info);
        let mut plot = Plot::new();
        let trace = Scatter::new(
            metrics.iter().map(|el| el.0.time_required.as_secs_f32()).collect(),
            metrics.iter().map(|el| el.0.compressed_size).collect())
            .name(format!("Workload {}",workload_filename))
            .text_array(metrics.iter().map(|el| el.0.algorithm.name()).collect());
        plot.add_trace(trace);

        plot.write_html(format!("results/convex-hull-{}.html",workload_filename));

        let mut plot = Plot::new();
        let trace = Bar::new(
            metrics.iter().skip(1).enumerate().map(|(i, _)| i + 2).collect(), // +1 due to the skip and +1 since we start from 0
            metrics.iter().skip(1).map(|el| el.1).collect())
            .name(format!("Workload '{}'",workload_filename));

        plot.add_trace(trace);

        plot.write_html(format!("results/benefit-{}.html",workload_filename));
    }

    let mut plot = Plot::new();
    let trace = Scatter::new(
        mixing_policy.lower_convex_hull.iter().map(|metric| {
            // we're analyzing a combination
            metric.0.iter().fold(0., |acc, setup| acc + setup.0.time_required.as_secs_f32())
        }).collect(),
        mixing_policy.lower_convex_hull.iter().map(|metric| {
            // we're analyzing a combination
            metric.0.iter().fold(0, |acc, setup| acc + setup.0.compressed_size)
        }).collect())
        .name("Merged convex hull")
        .text_array(mixing_policy.lower_convex_hull.iter().map(|el| {
            let setup_names: Vec<_> = el.0.iter().map(|el| el.0.algorithm.name()).collect();
            format!("({})", setup_names.join(","))
        }).collect());
    plot.add_trace(trace);

    plot.write_html("results/result.html");

    let mut plot = Plot::new();
    let trace = Bar::new(
        mixing_policy.lower_convex_hull.iter().skip(1).map(|metric| metric.2.clone()).collect(),
        mixing_policy.lower_convex_hull.iter().skip(1).map(|metric| metric.1.log2()).collect())
        .name("Merged convex hull");
    plot.add_trace(trace);

    plot.write_html("results/result-benefit.html");
    let mut result_info = "Result".to_string();
    for metrics in &mixing_policy.lower_convex_hull {
        let display: Vec<_> = metrics.0.iter().map(|metric| (metric.0.time_required.as_secs_f64(), metric.0.compressed_size)).collect();
        result_info.push_str(&*format!("\n{:?}", display));
    }
    log::info!("{}", result_info);

    let optimal_mixes = mixing_policy.mix_with_total_time_budget(total_time_budget);
    match optimal_mixes {
        Some(optimal_mixes) => {
            optimal_mixes
                .iter()
                .enumerate()
                .zip(workloads)
                .zip(&workload_filenames)
                .for_each(|(((index, mix), work), filename)| {
                let compressed_workload = MixingPolicy::apply_optimal_mix(mix, &work);
                fs::write(format!("results/multiple-docs-{}.zip", filename), compressed_workload).expect(format!("Couldn't write to path 'results/multiple-docs-{}.zip'", filename).as_str());
            })
        },
        None => ()
    }
}