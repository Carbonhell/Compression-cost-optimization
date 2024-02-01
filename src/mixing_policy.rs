use std::time::{Duration, Instant};
use crate::algorithms::AlgorithmMetrics;
use crate::convex_hull::convex_hull_graham;
use crate::workload::Workload;

pub type MetricsWithBenefit<'a> = (&'a AlgorithmMetrics, f64);
/// Also stores an identifier of the combination
pub type CombinationWithBenefit<'a> = (Vec<MetricsWithBenefit<'a>>, f64, String);

pub struct MixingPolicy<'a> {
    pub lower_convex_hull: Vec<MetricsWithBenefit<'a>>,
}

pub struct MixingPolicyMultipleWorkloads<'a> {
    pub lower_convex_hull: Vec<CombinationWithBenefit<'a>>,
    pub lower_convex_hull_per_workload: Vec<Vec<MetricsWithBenefit<'a>>>,
}

impl MixingPolicyMultipleWorkloads<'_> {
    pub fn new(algorithm_metrics: Vec<Vec<&AlgorithmMetrics>>) -> MixingPolicyMultipleWorkloads {
        let mut setup_combinations = Vec::new();
        let mut workload_lchs_by_benefit: Vec<Vec<MetricsWithBenefit>> = Vec::with_capacity(algorithm_metrics.len());
        let mut current_combination = Vec::with_capacity(algorithm_metrics.len());
        let mut raw_workload_lchs: Vec<Vec<MetricsWithBenefit>> = Vec::with_capacity(algorithm_metrics.len());
        for (index, metrics) in algorithm_metrics.into_iter().enumerate() {
            log::info!("Building lower convex hull for metrics #{}", index);
            let lower_convex_hull = MixingPolicy::build_polygonal_chain(metrics);
            raw_workload_lchs.push(lower_convex_hull.clone());
            current_combination.push(lower_convex_hull[0]);
            workload_lchs_by_benefit.push(lower_convex_hull.into_iter().skip(1).collect());
        }
        log::debug!("Initial combination: {:?}", setup_combinations);
        for (index, lch) in workload_lchs_by_benefit.iter().enumerate() {
            let mut x = format!("LCH of index {}", index);
            for metric in lch {
                x.push_str(format!("\n{:?};{:?}", metric.0.time_required.as_secs_f64(), metric.0.compressed_size).as_ref())
            }
            log::debug!("{}", x);
        }

        // Initial combination of initial useful setups for each doc
        let mut previous_complessive_time = 0.;
        let mut previous_complessive_size = 0;
        setup_combinations.push((current_combination.clone(), 0., "initial".to_string()));
        while !workload_lchs_by_benefit.iter().all(|x| x.is_empty()) {
            log::debug!("New lchs iteration: {:?}", workload_lchs_by_benefit);
            let (index, max_setup_across_workloads) = workload_lchs_by_benefit
                .iter_mut()
                .enumerate()
                // Ignore workload setups that have been fully processed (note: we don't remove the empty vec because it'd mess with the index we use to figure out which workload the lch refers to)
                .filter(|(_, workload_setups)| !workload_setups.is_empty())
                .max_by(|(_, lch1), (_, lch2)| lch1[0].1.total_cmp(&lch2[0].1))
                .unwrap(); // We can safely unwrap since the while condition prevents an empty result of max_by
            let highest_benefit_setup = max_setup_across_workloads.remove(0);
            log::debug!("The maximum setup is at index {}, {:?}", index, highest_benefit_setup);
            let combination_variation = format!("Workload #{} - {}", index, highest_benefit_setup.0.algorithm.name());
            current_combination[index] = highest_benefit_setup;

            let combination_time = current_combination.iter().fold(0., |acc, setup| acc + setup.0.time_required.as_secs_f64());
            let combination_size = current_combination.iter().fold(0, |acc, setup| acc + setup.0.compressed_size);
            let combination_benefit = (previous_complessive_size - combination_size) as f64 / (combination_time - previous_complessive_time);
            previous_complessive_size = combination_size;
            previous_complessive_time = combination_time;

            setup_combinations.push((current_combination.clone(), combination_benefit, combination_variation));
            if max_setup_across_workloads.is_empty() {
                log::debug!("LCH of index {} cleared", index);
            }
        }

        MixingPolicyMultipleWorkloads {
            lower_convex_hull: setup_combinations,
            lower_convex_hull_per_workload: raw_workload_lchs,
        }
    }

    /// Returns the mixes to apply to each workload to respect the total time budget provided.
    /// At all times, only at most one workload will consist of an actual mix between useful setups. All the other workloads will only use one specific setup.
    /// For now, only the total time budget is taken into account. The workload time budget is ignored.
    pub fn mix_with_total_time_budget(&self, total_time_budget: Duration) -> Option<Vec<OptimalMix>> {
        log::debug!("Calling mix_with_total_time_budget, {:?}", self.lower_convex_hull);
        let optimal_combination: Option<Vec<_>> = self
            .lower_convex_hull
            .windows(2)
            .find(|combination_pair| {
                let (prev, curr) = (&combination_pair[0], &combination_pair[1]);
                let prev_total_required_time = prev
                    .0
                    .iter()
                    .fold(Duration::from_secs(0), |acc, metric| acc + metric.0.time_required);

                let curr_total_required_time = curr
                    .0
                    .iter()
                    .fold(Duration::from_secs(0), |acc, metric| acc + metric.0.time_required);

                if total_time_budget >= prev_total_required_time && total_time_budget < curr_total_required_time {
                    return true;
                }
                false
            })
            .map(|group| {
                let (expensive_combination, cheap_combination) = (&group[1].0, &group[0].0);

                let expensive_total_required_time = expensive_combination
                    .iter()
                    .fold(Duration::from_secs(0), |acc, metric| acc + metric.0.time_required);

                let cheap_total_required_time = cheap_combination
                    .iter()
                    .fold(Duration::from_secs(0), |acc, metric| acc + metric.0.time_required);

                // Merge the two combinations in a single vec of optimal mixes
                cheap_combination
                    .iter()
                    .zip(expensive_combination)
                    .map(|(cheap_metric, expensive_metric)| {
                        if cheap_metric == expensive_metric {
                            OptimalMix::Single(cheap_metric.0)
                        } else {
                            let fraction = (total_time_budget.as_secs_f64() - cheap_total_required_time.as_secs_f64()) / (expensive_total_required_time.as_secs_f64() - cheap_total_required_time.as_secs_f64());
                            let fraction = (fraction * 100.).round();
                            OptimalMix::Normal((expensive_metric.0, cheap_metric.0), fraction / 100.)
                        }
                    })
                    .collect()
            });
        let optimal_combination = if let None = optimal_combination {
            log::debug!("Checking the most expensive combination");
            let most_expensive_combination = self
                .lower_convex_hull
                .last()
                .unwrap();
            let total_required_time = most_expensive_combination
                .0
                .iter()
                .fold(Duration::from_secs(0), |acc, metric| acc + metric.0.time_required);

            if total_required_time < total_time_budget {
                Some(most_expensive_combination
                    .0
                    .iter()
                    .map(|metric| OptimalMix::Single(metric.0))
                    .collect::<Vec<_>>())
            } else {
                None
            }
        } else {
            optimal_combination
        };
        log::debug!("Optimal combination: {:?}", optimal_combination);
        optimal_combination
    }

    pub fn apply_optimal_combination(optimal_mixes: &Vec<OptimalMix>, workloads: &mut Vec<Workload>, total_time_budget: Duration) {
        log::info!("Applying optimal combination");
        let instant = Instant::now();
        optimal_mixes
            .iter()
            .zip(workloads)
            .for_each(|(optimal_mix, workload)| {
                match optimal_mix {
                    OptimalMix::Single(metrics) => {
                        let instant = Instant::now();
                        log::info!("Applying single algorithm for workload {}", workload.name);
                        let data = metrics.algorithm.execute(workload);
                        log::info!("Time passed for workload {}: {:?}", workload.name, instant.elapsed());
                        data
                    }
                    OptimalMix::Normal((metric_a, metric_b), fraction) => {
                        let workload_partition = ((workload.data.metadata().unwrap().len() as f64) * fraction).round() as usize;
                        log::info!("Applying mix of algorithms with fraction {} and partition at index {} (data len is {})", fraction, workload_partition, workload.data.metadata().unwrap().len());
                        let instant = Instant::now();
                        log::debug!("Applying optimal mix: before algorithm A {:?}", instant.elapsed());
                        metric_a.algorithm.execute_with_target(workload, workload_partition, true);
                        log::debug!("Applying optimal mix: after algorithm A, before B {:?}", instant.elapsed());
                        metric_b.algorithm.execute_with_target(workload, workload_partition, false);
                        log::info!("Time passed for workload {}: {:?}", workload.name, instant.elapsed());
                    }
                }
            });
        log::info!("Time passed for the application of all mixes: {:?} (should be near the time budget which is {:?})", instant.elapsed(), total_time_budget);
    }
}

impl MixingPolicy<'_> {
    pub fn new(algorithm_metrics: Vec<&AlgorithmMetrics>) -> MixingPolicy {
        MixingPolicy {
            lower_convex_hull: MixingPolicy::build_polygonal_chain(algorithm_metrics)
        }
    }

    fn build_polygonal_chain(mut algorithm_metrics: Vec<&AlgorithmMetrics>) -> Vec<MetricsWithBenefit> {
        if algorithm_metrics.len() < 1 {
            panic!("A mixing policy requires at least one algorithm.")
        }

        log::debug!("Building polygonal chain with metrics: {:?}", algorithm_metrics);

        algorithm_metrics.sort();
        log::debug!("Sorted metrics: {:?}", algorithm_metrics);
        // All groups of 3 metrics follow the standard algorithm. Can result in an empty iterator if we have less than 3 metrics.
        let polygonal_chain = algorithm_metrics
            .windows(3)
            .filter(|metric_group| !(metric_group[1].compressed_size >= metric_group[0].compressed_size || metric_group[1].time_required == metric_group[2].time_required))
            .map(|metric_group| metric_group[1]);

        log::debug!("Initial polygonal chain state: {:?}", polygonal_chain.clone().collect::<Vec<&AlgorithmMetrics>>());
        // The last algorithm is selected only on the basis of its compression ratio being better. Can result in an empty iterator if we only have 1 metric.
        let last_algorithm_group = algorithm_metrics
            .windows(2)
            .last();
        log::debug!("Last algorithm group: {:?}", last_algorithm_group);
        let last_algorithm = match last_algorithm_group {
            Some(metric_group) => {
                let res = if !(metric_group[1].compressed_size >= metric_group[0].compressed_size) {
                    Some(metric_group[1])
                } else {
                    None
                };
                res
            }
            None => None
        };
        log::debug!("Last algorithm: {:?}", last_algorithm);

        let polygonal_chain = polygonal_chain.chain(last_algorithm);

        log::debug!("Polygonal chain with last algorithm added: {:?}", polygonal_chain.clone().collect::<Vec<&AlgorithmMetrics>>());
        // The first algorithm is always selected.
        let polygonal_chain: Vec<_> = algorithm_metrics.first()
            .map(|m| *m)
            .into_iter()
            .chain(polygonal_chain)
            .collect();
        log::debug!("Polygonal chain with first algorithm added: {:?}", polygonal_chain);

        let convex_hull = convex_hull_graham(&polygonal_chain[..]);

        log::debug!("Convex hull: {:?}", convex_hull);
        // Graham's convex hull algorithm returns an ordered slice of points in counter-clockwise order.
        // We can use this property to easily get the lower polygonal chain by getting a sub slice from min x to max x
        let min_metric = convex_hull
            .iter()
            .min_by(|m1, m2| m1.time_required.cmp(&m2.time_required))
            .unwrap();
        let max_metric = convex_hull
            .iter()
            .max_by(|m1, m2| m1.time_required.cmp(&m2.time_required))
            .unwrap();
        log::debug!("Min: {:?}\nMax: {:?}", min_metric, max_metric);

        let polygonal_chain = convex_hull
            .iter()
            .cycle();
        let mut min_found = false;
        let mut lower_convex_hull: Vec<&AlgorithmMetrics> = Vec::new();
        for el in polygonal_chain {
            if !min_found {
                if el == min_metric {
                    min_found = true;
                    lower_convex_hull.push(el);
                }
            } else {
                lower_convex_hull.push(el);
                if el == max_metric {
                    break;
                }
            }
        }
        log::debug!("Lower convex hull: {:?}", lower_convex_hull);

        let mut hull_with_benefits = vec![(lower_convex_hull[0], 0.)];
        lower_convex_hull
            .windows(2)
            .for_each(|pair| {
                let (prev, curr) = (pair[0], pair[1]);
                hull_with_benefits.push(
                    (curr,
                     (prev.compressed_size - curr.compressed_size) as f64
                         / (curr.time_required.as_secs_f64() - prev.time_required.as_secs_f64())));
            });
        log::debug!("Lower convex hull with benefits: {:?}", hull_with_benefits);
        hull_with_benefits
    }

    /// Can result in a none if the workload time budget doesn't allow even for the cheapest algorithm to be ran
    pub fn optimal_mix(&self, workload: &Workload) -> Option<OptimalMix> {
        let optimal_mix = self
            .lower_convex_hull
            .windows(2)
            .find(|mix_group| {
                if workload.time_budget >= mix_group[0].0.time_required && workload.time_budget <= mix_group[1].0.time_required {
                    return true;
                }
                false
            })
            .map(|group| {
                let (expensive_alg, cheap_alg) = (group[1].0, group[0].0);
                log::debug!("Valid groups for optimal mix:\n{:?}\n{:?}", cheap_alg, expensive_alg);
                let fraction = (workload.time_budget.as_secs_f64() - cheap_alg.time_required.as_secs_f64()) / (expensive_alg.time_required.as_secs_f64() - cheap_alg.time_required.as_secs_f64());
                // Floating point calculation could result in two values not summing up evenly to 1, let's use u32s for this
                let fraction = (fraction * 100.).round();
                OptimalMix::Normal((expensive_alg, cheap_alg), fraction / 100.)
            });
        // Special case: time budget allows for the most expensive algorithm to be used
        let optimal_mix = if let None = optimal_mix {
            log::debug!("Checking the most expensive mix");
            let most_expensive_algorithm = self
                .lower_convex_hull
                .last()
                .unwrap()
                .0;
            if most_expensive_algorithm.time_required < workload.time_budget {
                Some(OptimalMix::Single(most_expensive_algorithm))
            } else {
                None
            }
        } else {
            optimal_mix
        };
        log::debug!("Optimal mix: {:?}", optimal_mix);
        optimal_mix
    }

    pub fn apply_optimal_mix(optimal_mix: &OptimalMix, workload: &mut Workload) {
        match optimal_mix {
            OptimalMix::Single(metrics) => {
                log::debug!("Applying single algorithm");
                metrics.algorithm.execute(workload)
            }
            OptimalMix::Normal((metric_a, metric_b), fraction) => {
                let workload_partition = ((workload.data.metadata().unwrap().len() as f64) * fraction).round() as usize;
                log::debug!("Applying mix of algorithms with fraction {} and partition at index {} (data len is {})", fraction, workload_partition, workload.data.metadata().unwrap().len());
                let instant = Instant::now();
                log::debug!("Applying optimal mix: before algorithm A {:?}", instant.elapsed());
                metric_a.algorithm.execute_with_target(workload, workload_partition, true);
                log::debug!("Applying optimal mix: after algorithm A, before B {:?}", instant.elapsed());
                metric_b.algorithm.execute_with_target(workload, workload_partition, false);
                log::info!("Time passed: {:?} (should be near the time budget which is {:?})", instant.elapsed(), workload.time_budget);
            }
        }
    }
}

#[derive(Debug)]
pub enum OptimalMix<'a> {
    /// The workload allows using only an extreme algorithm (the worst or the best), the fraction is obviously 1.
    Single(&'a AlgorithmMetrics),
    /// We got a proper mix, with each algorithm handling a fraction of the workload
    Normal((&'a AlgorithmMetrics, &'a AlgorithmMetrics), f64),
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::tempfile;
    use crate::algorithms::{Algorithm, AlgorithmMetrics, ByteSize};
    use crate::mixing_policy::MixingPolicy;
    use crate::workload::Workload;

    #[derive(Debug)]
    struct MockAlgorithm {
        pub compressed_size: ByteSize,
        pub time_required: Duration,
    }

    impl Algorithm for MockAlgorithm {
        fn name(&self) -> String {
            "Mock".to_string()
        }

        fn compressed_size(&self) -> ByteSize {
            self.compressed_size
        }

        fn time_required(&self) -> Duration {
            self.time_required
        }

        fn execute(&self, _: &mut Workload) {  }

        fn execute_on_tmp(&self, _: &mut Workload) -> File { tempfile().unwrap() }

        fn execute_with_target(&self, _: &mut Workload, _: usize, _: bool) {}
    }

    #[test]
    fn paper_polygonal_chain() {
        let _ = env_logger::try_init();
        let mut tmp = tempfile().unwrap();
        tmp.write_all("test".as_bytes()).unwrap();
        let workload = Workload::new(String::from("test"), tmp, Duration::from_secs(5));
        let algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 1_000_000, time_required: Duration::from_secs(2) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 800_000 as ByteSize, time_required: Duration::from_secs(4) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 600_000 as ByteSize, time_required: Duration::from_secs(6) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 580_000 as ByteSize, time_required: Duration::from_secs(7) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10) })),
        ];
        let algorithm_metrics = algorithm_metrics.iter().collect();
        let mixing_policy = MixingPolicy::new(algorithm_metrics);

        // Fetched on https://ch.mathworks.com/help/matlab/ref/convhull.html by using time_required as x and compressed_size as y, according to the paper plots
        let expected_algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 1_000_000, time_required: Duration::from_secs(2) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 800_000, time_required: Duration::from_secs(4) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 600_000 as ByteSize, time_required: Duration::from_secs(6) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10) })),
        ];
        let expected_algorithm_metrics: Vec<_> = expected_algorithm_metrics.iter().collect();
        let obtained_algorithm_metrics = mixing_policy.lower_convex_hull.iter().map(|el| el.0).collect::<Vec<&AlgorithmMetrics>>();
        assert_eq!(obtained_algorithm_metrics, expected_algorithm_metrics);
    }

    #[test]
    fn optimal_mix() {
        let _ = env_logger::try_init();
        let mut tmp = tempfile().unwrap();
        tmp.write_all("test".as_bytes()).unwrap();
        let workload = Workload::new(String::from("test"), tmp, Duration::from_secs(7));
        let algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 1_000_000, time_required: Duration::from_secs(2) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 800_000 as ByteSize, time_required: Duration::from_secs(4) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 600_000 as ByteSize, time_required: Duration::from_secs(6) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 580_000 as ByteSize, time_required: Duration::from_secs(7) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8) })),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10) })),
        ];
        let algorithm_metrics = algorithm_metrics.iter().collect();
        let mixing_policy = MixingPolicy::new(algorithm_metrics);
        println!("LCH: {:?}", mixing_policy.lower_convex_hull);
        println!("{:?}", mixing_policy.optimal_mix(&workload));
    }
}