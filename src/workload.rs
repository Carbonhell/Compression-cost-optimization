use std::time::Duration;

/// Defines the structure of a workload, containing the data to be compressed, the time budget and the algorithms to use.
pub struct Workload {
    data: String, // todo generic
    pub time_budget: Duration,
    //algorithms: Vec<Box<dyn Algorithm>> // todo way to specify those with generics?
}

impl Workload {
    pub fn new(data: String, time_budget: Duration) -> Self {
        Self { data, time_budget }
    }
}

