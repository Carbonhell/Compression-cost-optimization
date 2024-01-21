use std::io::Cursor;
use std::time::Instant;
use crate::algorithms::AlgorithmMetrics;
use crate::convex_hull::convex_hull_graham;
use crate::workload::Workload;

pub type MetricsWithBenefit<'a> = (&'a AlgorithmMetrics, f64);

pub struct MixingPolicy<'a> {
    pub lower_convex_hull: Vec<MetricsWithBenefit<'a>>,
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
        if let None = optimal_mix {
            let most_expensive_algorithm = self
                .lower_convex_hull
                .last()
                .unwrap()
                .0;
            if most_expensive_algorithm.time_required < workload.time_budget {
                return Some(OptimalMix::Single(most_expensive_algorithm));
            }
            return None;
        }
        log::debug!("Optimal mix: {:?}", optimal_mix);
        optimal_mix
    }

    pub fn apply_optimal_mix(optimal_mix: OptimalMix, workload: &Workload) -> Vec<u8> {
        match optimal_mix {
            OptimalMix::Single(metrics) => {
                log::debug!("Applying single algorithm");
                metrics.algorithm.execute(workload)
            }
            OptimalMix::Normal((metric_a, metric_b), fraction) => {
                let workload_partition = ((workload.data.len() as f64) * fraction).round() as usize;
                log::debug!("Applying mix of algorithms with fraction {} and partition at index {} (data len is {})", fraction, workload_partition, workload.data.len());
                let mut cursor = Cursor::new(Vec::with_capacity(workload.data.len()));
                let instant = Instant::now();
                log::debug!("Applying optimal mix: before algorithm A {:?}", instant.elapsed());
                metric_a.algorithm.execute_with_target(&Workload::new(&workload.data[..workload_partition], workload.time_budget), &mut cursor);
                log::debug!("Applying optimal mix: after algorithm A, before B {:?}", instant.elapsed());
                metric_b.algorithm.execute_with_target(&Workload::new(&workload.data[workload_partition..], workload.time_budget), &mut cursor);
                log::info!("Time passed: {:?} (should be near the time budget which is {:?})", instant.elapsed(), workload.time_budget);
                cursor.into_inner()
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
    use std::io::Cursor;
    use std::time::Duration;
    use crate::algorithms::{Algorithm, AlgorithmMetrics, ByteSize};
    use crate::mixing_policy::MixingPolicy;
    use crate::workload::Workload;

    #[derive(Debug)]
    struct MockAlgorithm {
        pub compressed_size: ByteSize,
        pub time_required: Duration,
    }

    impl Algorithm for MockAlgorithm {
        fn compressed_size(&mut self, _: &Workload) -> ByteSize {
            self.compressed_size
        }

        fn time_required(&mut self, _: &Workload) -> Duration {
            self.time_required
        }

        fn execute(&self, _: &Workload) -> Vec<u8> { Vec::new() }

        fn execute_with_target(&self, _w: &Workload, _target: &mut Cursor<Vec<u8>>) {}
    }

    #[test]
    fn paper_polygonal_chain() {
        env_logger::init();
        let workload = Workload::new("test".as_bytes(), Duration::from_secs(5));
        let algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 1_000_000, time_required: Duration::from_secs(2) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 800_000 as ByteSize, time_required: Duration::from_secs(4) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 600_000 as ByteSize, time_required: Duration::from_secs(6) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 580_000 as ByteSize, time_required: Duration::from_secs(7) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10) }), &workload),
        ];
        let algorithm_metrics = algorithm_metrics.iter().collect();
        let mixing_policy = MixingPolicy::new(algorithm_metrics);

        // Fetched on https://ch.mathworks.com/help/matlab/ref/convhull.html by using time_required as x and compressed_size as y, according to the paper plots
        let expected_algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 1_000_000, time_required: Duration::from_secs(2) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 800_000, time_required: Duration::from_secs(4) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 600_000 as ByteSize, time_required: Duration::from_secs(6) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10) }), &workload),
        ];
        let expected_algorithm_metrics: Vec<_> = expected_algorithm_metrics.iter().collect();
        let obtained_algorithm_metrics = mixing_policy.lower_convex_hull.iter().map(|el| el.0).collect::<Vec<&AlgorithmMetrics>>();
        assert_eq!(obtained_algorithm_metrics, expected_algorithm_metrics);
    }

    #[test]
    fn optimal_mix() {
        env_logger::init();
        let workload = Workload::new("test".as_bytes(), Duration::from_secs(7));
        let algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 1_000_000, time_required: Duration::from_secs(2) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 800_000 as ByteSize, time_required: Duration::from_secs(4) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 600_000 as ByteSize, time_required: Duration::from_secs(6) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 580_000 as ByteSize, time_required: Duration::from_secs(7) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8) }), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm { compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10) }), &workload),
        ];
        let algorithm_metrics = algorithm_metrics.iter().collect();
        let mixing_policy = MixingPolicy::new(algorithm_metrics);
        println!("LCH: {:?}", mixing_policy.lower_convex_hull);
        println!("{:?}", mixing_policy.optimal_mix(&workload));
    }
}