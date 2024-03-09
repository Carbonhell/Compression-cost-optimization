use std::fs::{create_dir, create_dir_all, File, read_dir, ReadDir};
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
    pub fn new(name: String, data: File, time_budget: Duration, result_file: Option<File>) -> Self {
        let result_file = result_file.unwrap_or_else(|| File::create(format!("results/{}.zip", name))
            .expect(format!("Couldn't create result file for workload \"{}\"", name).as_str()));
        Self { name, data, time_budget, result_file }
    }
}


#[derive(Debug)]
pub struct FolderWorkload {
    pub name: String,
    pub time_budget: Duration,
}

impl FolderWorkload {
    pub fn new(name: String, time_budget: Duration) -> Self {
        create_dir(format!("results/{}", name))
            .expect(format!("Couldn't create result folder for workload \"{}\"", name).as_str());
        Self { name, time_budget }
    }

    pub fn get_data_folder(&self) -> ReadDir {
        read_dir(format!("data/{}", self.name))
            .expect(format!("Couldn't read data folder for workload \"{}\"", self.name).as_str())
    }

    pub fn data_files_count(&self) -> usize {
        let data_folder = self.get_data_folder();
        data_folder.count()
    }

    pub fn data_files_size(&self) -> u64 {
        let data_folder = self.get_data_folder();
        let mut size = 0;
        for path in data_folder {
            size += path.unwrap().metadata().unwrap().len();
        }
        size
    }

    pub fn get_results_folder(&self) {

    }
}