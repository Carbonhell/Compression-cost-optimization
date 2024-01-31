use std::fs;
use std::time::Duration;
use plotly::{Bar, Layout, Plot, Scatter};
use plotly::common::Title;
use plotly::layout::{Axis, Legend};
use crate::algorithms::{Algorithm, AlgorithmMetrics, ByteSize};
use crate::mixing_policy::{MetricsWithBenefit, MixingPolicy, MixingPolicyMultipleWorkloads};
use crate::workload::Workload;

pub mod workload;
pub mod algorithms;
mod mixing_policy;
mod convex_hull;

/// Find the optimal setups for a given document and time budget, and apply them. The result will be written in the `results` folder.
///
pub fn process_single_document(filename: &str, time_budget: f64, algorithms: Vec<Box<dyn Algorithm>>) {
    let workload_data = fs::read(format!("data/{}", filename)).expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.");
    let workload = Workload::new(String::from(filename), &workload_data, Duration::from_secs_f64(time_budget));
    log::debug!("Workload size: {:?}, time budget: {:?}", workload.data.len(), workload.time_budget);
    let algorithms: Vec<_> = algorithms
        .into_iter()
        .map(|alg| {
            AlgorithmMetrics::new(alg, &workload)
        })
        .collect();
    let mixing_policy = MixingPolicy::new(algorithms.iter().collect());
    draw_workload_plots(&mixing_policy.lower_convex_hull, filename);

    let optimal_mix = mixing_policy.optimal_mix(&workload);
    match optimal_mix {
        Some(optimal_mix) => {
            let compressed_workload = MixingPolicy::apply_optimal_mix(&optimal_mix, &workload);
            fs::write(format!("results/{}.zip", filename), compressed_workload).expect(format!("Couldn't write to path 'results/{}.zip'", filename).as_str());
        }
        None => {
            let minimum_time_budget = mixing_policy
                .lower_convex_hull
                .iter()
                .map(|el| el.0)
                .min();
            match minimum_time_budget {
                Some(min) => {
                    log::info!("No algorithm found that can compress data in the given time budget (Budget is {:?}, cheapest algorithm requires {:?}).", workload.time_budget, min.time_required)
                }
                None => {
                    log::info!("The lower convex hull is empty. Is this an error?");
                }
            }
        }
    }
}

pub fn process_multiple_documents(workload_filenames: Vec<String>, workload_algorithms: Vec<Vec<Box<dyn Algorithm>>>, total_time_budget: Duration) {
    let workload_payloads: Vec<_> = workload_filenames
        .iter()
        .map(|filename| {
            let data: Vec<u8> = fs::read(format!("data/{}", filename))
                .expect("Missing data file. Ensure the file exists and that it has been correctly placed in the project data folder.");
            data
        })
        .collect();
    let workloads: Vec<_> = workload_payloads
        .iter()
        .zip(&workload_filenames)
        .map(|(data, filename)| {
            Workload::new(filename.clone(), &data, Duration::from_secs_f64(0.)) // Workload duration is unused since we have a total time budget
        })
        .collect();

    let mut algorithms = Vec::new();
    workload_algorithms
        .into_iter()
        .zip(&workloads)
        .for_each(|(algorithm, workload)| {
            let compression_configurations: Vec<_> = algorithm
                .into_iter()
                .map(|alg| {
                    log::info!("Calculating metrics for algorithm {:?}, workload {:?}", alg.name(), workload.name);
                    AlgorithmMetrics::new(alg, &workload)
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
        let mut lch_info = format!("Metrics for workload '{}'\n(time in s. ; compressed size)", workload_filename);
        for metric in metrics {
            lch_info.push_str(&*format!("\n{} ; {} (benefit: {})", metric.0.time_required.as_secs_f32(), metric.0.compressed_size, metric.1));
        }
        log::info!("{}", lch_info);

        draw_workload_plots(metrics, workload_filename);
    }


    draw_multiple_workloads_plots(&algorithms, &mixing_policy, &workload_filenames);
    let mut result_info = "Resulting lower convex hull for the multiple document mix:".to_string();
    for metrics in &mixing_policy.lower_convex_hull {
        let display: Vec<_> = metrics.0.iter().map(|metric| (metric.0.time_required.as_secs_f64(), metric.0.compressed_size)).collect();
        result_info.push_str(&*format!("\n{:?} (benefit: {})", display, metrics.1));
    }
    log::info!("{}", result_info);

    // Apply the actual mix and write the resulting compressed data in the results folder
    let optimal_mixes = mixing_policy.mix_with_total_time_budget(total_time_budget);
    match optimal_mixes {
        Some(optimal_mixes) => {
            let results = MixingPolicyMultipleWorkloads::apply_optimal_combination(&optimal_mixes, &workloads, total_time_budget);
            results
                .iter()
                .zip(&workload_filenames)
                .for_each(|(data, filename)| {
                    fs::write(format!("results/multiple-docs-{}.zip", filename), data).expect(format!("Couldn't write to path 'results/multiple-docs-{}.zip'", filename).as_str());
                });
        }
        None => {
            let minimum_time_budget = mixing_policy
                .lower_convex_hull
                .iter()
                .map(|metric| {
                    // we're analyzing a combination
                    metric.0.iter().fold(0., |acc, setup| acc + setup.0.time_required.as_secs_f32())
                })
                .min_by(|a, b| a.total_cmp(b));
            match minimum_time_budget {
                Some(min) => {
                    log::info!("No algorithm found that can compress data in the given time budget (Budget is {:?}, cheapest algorithm requires {:?}).", total_time_budget, Duration::from_secs_f32(min))
                }
                None => {
                    log::warn!("The lower convex hull is empty. Is this an error?");
                }
            }
        }
    }
}

/// Draws convex hull and benefit plots for a MixingPolicyMultipleWorkloads struct,
/// with a comparison with a naive approach using the same compression level for each algorithm in each combination.
fn draw_multiple_workloads_plots(algorithms: &Vec<Vec<AlgorithmMetrics>>, mixing_policy: &MixingPolicyMultipleWorkloads, workload_filenames: &Vec<String>) {
    // Convex hull plot for the whole multiple document mixing process
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Convex hull of workloads \"{}\"", workload_filenames.join(","))))
        .x_axis(Axis::new().title(Title::new("Time (sec)")))
        .y_axis(Axis::new().title(Title::new("Size (bytes)")))
        .legend(Legend::new()));
    let trace = Scatter::new(
        mixing_policy.lower_convex_hull.iter().map(|metric| {
            // we're analyzing a combination
            metric.0.iter().fold(0., |acc, setup| acc + setup.0.time_required.as_secs_f32())
        }).collect(),
        mixing_policy.lower_convex_hull.iter().map(|metric| {
            metric.0.iter().fold(0, |acc, setup| acc + setup.0.compressed_size)
        }).collect())
        .text_template(".3s")
        .name("Merged convex hull")
        .text_array(mixing_policy.lower_convex_hull.iter().map(|el| {
            let setup_names: Vec<_> = el.0.iter().map(|el| el.0.algorithm.name()).collect();
            format!("({})", setup_names.join(","))
        }).collect());
    plot.add_trace(trace);

    // Comparison trace with naive combination mixing (same level of each algorithm)
    let max_alg_levels = algorithms
        .iter()
        .map(|metrics| metrics.len())
        .max()
        .unwrap();
    let naive_x = algorithms
        .iter()
        .map(|metrics| {
            let mut times = metrics
                .iter()
                .map(|metric| metric.time_required.as_secs_f64())
                .collect::<Vec<_>>();
            if times.len() < max_alg_levels {
                let last_time = *times.last().unwrap();
                for _ in 0..(max_alg_levels - times.len()) {
                    times.push(last_time);
                }
            }
            log::debug!("Pre-fold naive times: {:?}", times);
            times
        })
        .fold(vec![0.; max_alg_levels], |acc: Vec<f64>, times| {
            let x = acc
                .into_iter()
                .zip(times)
                .map(|(x, y)| {
                    x + y
                })
                .collect();
            x
        });
    let naive_y = algorithms
        .iter()
        .map(|metrics| {
            let mut compressed_sizes = metrics
                .iter()
                .map(|metric| metric.compressed_size)
                .collect::<Vec<_>>();
            if compressed_sizes.len() < max_alg_levels {
                let last_compressed_size = *compressed_sizes.last().unwrap();
                for _ in 0..(max_alg_levels - compressed_sizes.len()) {
                    compressed_sizes.push(last_compressed_size);
                }
            }
            log::debug!("Pre-fold naive comp: {:?}", compressed_sizes);
            compressed_sizes
        })
        .fold(vec![0; max_alg_levels], |acc: Vec<ByteSize>, times| {
            acc
                .into_iter()
                .zip(times)
                .map(|(x, y)| {
                    x + y
                })
                .collect()
        });

    log::debug!("Plotting naive mixes data:\n{:?}\n{:?}", naive_x, naive_y);
    let trace_naive = Scatter::new(naive_x, naive_y)
        .text_template(".3s")
        .name("Naive mix")
        .text_array((0..=max_alg_levels).map(|x| format!("Level {}", x)).collect());
    plot.add_trace(trace_naive);

    plot.write_html("results/result.html");

    // Benefit plot for the whole multiple document mixing process
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Benefits for workloads \"{}\"", workload_filenames.join(","))))
        .x_axis(Axis::new().title(Title::new("Useful setup")).dtick(1.))
        .y_axis(Axis::new().title(Title::new("Benefit (bytes/sec)")).tick_format(".3s"))
        .legend(Legend::new()));
    let trace = Bar::new(
        mixing_policy.lower_convex_hull.iter().skip(1).map(|metric| metric.2.clone()).collect(),
        mixing_policy.lower_convex_hull.iter().skip(1).map(|metric| metric.1.log2()).collect());
    plot.add_trace(trace);

    plot.write_html("results/result-benefit.html");
}

/// Draws two plots, one showing the convex hull associated to the provided metrics and one showing the benefits.
fn draw_workload_plots(metrics: &Vec<MetricsWithBenefit>, workload_filename: &str) {
    // Convex hull plot for a specific workload
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Convex hull of workload \"{}\"", workload_filename)))
        .x_axis(Axis::new().title(Title::new("Time (sec)")))
        .y_axis(Axis::new().title(Title::new("Size (bytes)")))
        .legend(Legend::new()));

    let trace = Scatter::new(
        metrics.iter().map(|el| el.0.time_required.as_secs_f32()).collect(),
        metrics.iter().map(|el| el.0.compressed_size).collect())
        .text_template(".3s")
        .name(format!("Workload {}", workload_filename))
        .text_array(metrics.iter().map(|el| el.0.algorithm.name()).collect());
    plot.add_trace(trace);

    plot.write_html(format!("results/convex-hull-{}.html", workload_filename));

    // Benefit plot for a specific workload
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Benefits for workload \"{}\"", workload_filename)))
        .x_axis(Axis::new().title(Title::new("Useful setup")).dtick(1.))
        .y_axis(Axis::new().title(Title::new("Benefit (bytes/sec)")).tick_format(".3s"))
        .legend(Legend::new()));
    let trace = Bar::new(
        metrics.iter().skip(1).enumerate().map(|(i, _)| (i + 2) as u32).collect(), // +1 due to the skip and +1 since we start from 0
        metrics.iter().skip(1).map(|el| el.1).collect())
        .name(format!("Workload '{}'", workload_filename));

    plot.add_trace(trace);

    plot.write_html(format!("results/benefit-{}.html", workload_filename));
}

