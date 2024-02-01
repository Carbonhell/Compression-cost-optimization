use std::fs::File;
use std::time::Duration;

/// Defines the structure of a workload, containing the data to be compressed, the time budget and the algorithms to use.
#[derive(Debug)]
pub struct Workload {
    pub name: String,
    pub data: File,
    pub time_budget: Duration,
    pub result_file: File
}

impl Workload {
    pub fn new(name: String, data: File, time_budget: Duration) -> Self {
        let result_file = File::create(format!("results/{}.zip", name))
            .expect(format!("Couldn't create result file for workload \"{}\"", name).as_str());
        Self { name, data, time_budget, result_file }
    }
}

