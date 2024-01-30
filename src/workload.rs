use std::time::Duration;

/// Defines the structure of a workload, containing the data to be compressed, the time budget and the algorithms to use.
#[derive(Debug)]
pub struct Workload<'a> {
    pub name: String,
    pub data: &'a [u8],
    pub time_budget: Duration,
}

impl<'a> Workload<'a> {
    pub fn new(name: String, data: &'a [u8], time_budget: Duration) -> Self {
        Self { name, data, time_budget }
    }
}

