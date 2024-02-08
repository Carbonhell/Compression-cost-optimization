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
pub fn process_single_document(mut workload: Workload, algorithms: Vec<Box<dyn Algorithm>>) {
    log::debug!("Workload size: {:?}, time budget: {:?}", workload.data.metadata().unwrap().len(), workload.time_budget);
    let algorithms: Vec<_> = algorithms
        .into_iter()
        .map(|alg| {
            AlgorithmMetrics::new(alg)
        })
        .collect();
    let mixing_policy = MixingPolicy::new(algorithms.iter().collect());
    draw_workload_plots(&mixing_policy.lower_convex_hull, &workload.name);

    let optimal_mix = mixing_policy.optimal_mix(&workload);
    match optimal_mix {
        Some(optimal_mix) => {
            MixingPolicy::apply_optimal_mix(&optimal_mix, &mut workload);
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

pub fn process_multiple_documents(mut workloads: Vec<Workload>, workload_algorithms: Vec<Vec<Box<dyn Algorithm>>>, total_time_budget: Duration) {
    let mut algorithms = Vec::new();
    workload_algorithms
        .into_iter()
        .for_each(|algorithm| {
            let compression_configurations: Vec<_> = algorithm
                .into_iter()
                .map(|alg| {
                    AlgorithmMetrics::new(alg)
                })
                .collect();
            algorithms.push(compression_configurations);
        });

    // TODO sort out the borrow issue with &AlgorithmMetrics to remove this hack
    let alg2 = algorithms.iter().map(|el| el.iter().collect()).collect();
    let mixing_policy = MixingPolicyMultipleWorkloads::new(alg2);

    for (metrics, workload) in mixing_policy
        .lower_convex_hull_per_workload
        .iter()
        .zip(&workloads) {
        let mut lch_info = format!("Metrics for workload '{}'\n(time in s. ; compressed size)", workload.name);
        for metric in metrics {
            lch_info.push_str(&*format!("\n{} ; {} (benefit: {})", metric.0.time_required.as_secs_f32(), metric.0.compressed_size, metric.1));
        }
        log::info!("{}", lch_info);

        draw_workload_plots(metrics, &workload.name);
    }


    draw_multiple_workloads_plots(&algorithms, &mixing_policy, &workloads);
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
            MixingPolicyMultipleWorkloads::apply_optimal_combination(&optimal_mixes, &mut workloads, total_time_budget);
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
fn draw_multiple_workloads_plots(algorithms: &Vec<Vec<AlgorithmMetrics>>, mixing_policy: &MixingPolicyMultipleWorkloads, workload_filenames: &Vec<Workload>) {
    let workload_filenames = workload_filenames.iter().map(|el| el.name.clone()).collect::<Vec<_>>().join(",");
    // Convex hull plot for the whole multiple document mixing process
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Convex hull of workloads \"{}\"", workload_filenames)))
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
    let tags = algorithms
        .iter()
        .map(|metrics| {
            metrics.iter().map(|metric| metric.algorithm.name()).collect::<Vec<_>>()
        })
        .reduce(|acc, el| {
            acc
                .iter()
                .zip(el)
                .map(|(prev, curr)| {
                    let mut new_str = prev.clone();
                    new_str.push_str(&format!(", {}", curr));
                    new_str
                })
                .collect::<Vec<_>>()
        }).unwrap();

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
        .text_array(tags);
    plot.add_trace(trace_naive);

    plot.write_html("results/result.html");

    // Benefit plot for the whole multiple document mixing process
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Benefits for workloads \"{}\"", workload_filenames)))
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
fn draw_workload_plots(metrics: &Vec<MetricsWithBenefit>, workload_name: &str) {
    // Convex hull plot for a specific workload
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Convex hull of workload \"{}\"", workload_name)))
        .x_axis(Axis::new().title(Title::new("Time (sec)")))
        .y_axis(Axis::new().title(Title::new("Size (bytes)")))
        .legend(Legend::new()));

    let trace = Scatter::new(
        metrics.iter().map(|el| el.0.time_required.as_secs_f32()).collect(),
        metrics.iter().map(|el| el.0.compressed_size).collect())
        .text_template(".3s")
        .name(format!("Workload {}", workload_name))
        .text_array(metrics.iter().map(|el| el.0.algorithm.name()).collect());
    plot.add_trace(trace);

    plot.write_html(format!("results/convex-hull-{}.html", workload_name));

    // Benefit plot for a specific workload
    let mut plot = Plot::new();
    plot.set_layout(Layout::new()
        .title(Title::new(&*format!("Benefits for workload \"{}\"", workload_name)))
        .x_axis(Axis::new().title(Title::new("Useful setup")).dtick(1.))
        .y_axis(Axis::new().title(Title::new("Benefit (bytes/sec)")).tick_format(".3s"))
        .legend(Legend::new()));
    let trace = Bar::new(
        metrics.iter().skip(1).enumerate().map(|(i, _)| (i + 2) as u32).collect(), // +1 due to the skip and +1 since we start from 0
        metrics.iter().skip(1).map(|el| el.1).collect())
        .name(format!("Workload '{}'", workload_name));

    plot.add_trace(trace);

    plot.write_html(format!("results/benefit-{}.html", workload_name));
}

