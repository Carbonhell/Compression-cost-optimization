use crate::algorithms::AlgorithmMetrics;
use crate::convex_hull::convex_hull_graham;
use crate::workload::Workload;

pub struct MixingPolicy<'a> {
    pub polygonal_chain: Vec<&'a AlgorithmMetrics>
}

impl MixingPolicy<'_> {
    pub fn new(algorithm_metrics: Vec<&AlgorithmMetrics>) -> MixingPolicy {
        MixingPolicy {
            polygonal_chain: MixingPolicy::build_polygonal_chain(algorithm_metrics)
        }
    }

    fn build_polygonal_chain(mut algorithm_metrics: Vec<&AlgorithmMetrics>) -> Vec<&AlgorithmMetrics> {
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

        let polygonal_chain = convex_hull_graham(&polygonal_chain[..]);
        log::debug!("Convex hull: {:?}", polygonal_chain);
        // Graham's convex hull algorithm returns an ordered slice of points in counter-clockwise order.
        // We can use this property to easily get the lower polygonal chain by getting a sub slice from min x to max x
        let min_metric = polygonal_chain
            .iter()
            .min_by(|m1, m2| m1.compressed_size.cmp(&m2.compressed_size))
            .unwrap();
        let max_metric = polygonal_chain
            .iter()
            .max_by(|m1, m2| m1.compressed_size.cmp(&m2.compressed_size))
            .unwrap();
        log::debug!("Min: {:?}\nMax: {:?}", min_metric, max_metric);

        let polygonal_chain = polygonal_chain
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
        lower_convex_hull
    }

    fn optimal_mix(&self, workload: &Workload) -> Option<OptimalMix> {
        let optimal_mix = self
            .polygonal_chain
            .windows(2)
            .find(|mix_group| {
                if workload.time_budget >= mix_group[0].time_required && workload.time_budget <= mix_group[1].time_required {
                    return true
                }
                false
            })
            .map(|group| OptimalMix::Normal((group[0], group[1])));
        // Special case: time budget allows for the most expensive algorithm to be used
        if let None = optimal_mix {
            let most_expensive_algorithm = self
                .polygonal_chain
                .last()
                .unwrap();
            if most_expensive_algorithm.time_required < workload.time_budget {
                return Some(OptimalMix::Single(most_expensive_algorithm));
            }
            return None;
        }
        optimal_mix
    }
}

pub enum OptimalMix<'a> {
    Single(&'a AlgorithmMetrics),
    Normal((&'a AlgorithmMetrics, &'a AlgorithmMetrics))
}

#[cfg(test)]
mod tests {
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
        fn compressed_size(&self, _: &Workload) -> ByteSize {
            self.compressed_size
        }

        fn time_required(&self, _: &Workload) -> Duration {
            self.time_required
        }

        fn execute(&self, _: &Workload) {}
    }

    #[test]
    fn paper_polygonal_chain() {
        env_logger::init();
        let workload = Workload::new("test".to_string(), Duration::from_secs(5));
        let algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 1_000_000, time_required: Duration::from_secs(2)}), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 800_000 as ByteSize, time_required: Duration::from_secs(4)}), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 600_000 as ByteSize, time_required: Duration::from_secs(6)}), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 580_000 as ByteSize, time_required: Duration::from_secs(7)}), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8)}), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10)}), &workload),
        ];
        let algorithm_metrics = algorithm_metrics.iter().collect();
        let mixing_policy = MixingPolicy::new(algorithm_metrics);

        let expected_algorithm_metrics = vec![
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 300_000 as ByteSize, time_required: Duration::from_secs(10)}), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 400_000 as ByteSize, time_required: Duration::from_secs(8)}), &workload),
            AlgorithmMetrics::new(Box::new(MockAlgorithm{compressed_size: 1_000_000, time_required: Duration::from_secs(2)}), &workload),
        ];
        let expected_algorithm_metrics: Vec<_> = expected_algorithm_metrics.iter().collect();
        assert_eq!(mixing_policy.polygonal_chain, expected_algorithm_metrics);
    }
}