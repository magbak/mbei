/*Copyright 2022 Prediktor AS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::Duration;
use log::{debug, info};

use mbei_testdata::factory_scenario_builder::{complex_factory_scenario_builder, create_simple_factory_scenario};
use mbei_testdata::producer::{TestdataProducer};
use structopt::StructOpt;
use mbei_testdata::complex_factory_scenario::ComplexFactoryScenarioSimulator;
use mbei_testdata::simple_factory_scenario::{SimpleFactoryScenarioSimulator};

#[derive(StructOpt)]
pub struct ComplexFactoryCommandArgs {
    #[structopt(short = "-s", long = "--size")]
    pub size: u32,
}

#[derive(StructOpt)]
pub enum Command {
    #[structopt(name = "simple-factory")]
    SimpleFactoryConfig,
    #[structopt(name = "complex-factory")]
    ComplexFactoryConfig(ComplexFactoryCommandArgs),
}

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(subcommand)]
    pub command: Command,

    #[structopt(short = "-m", long = "--message-rate")]
    pub messages_per_second: u32,

    #[structopt(short = "-u", long = "--names-url-path", parse(from_os_str))]
    pub names_url_path: std::path::PathBuf,

    #[structopt(short = "-n", long = "--number_of_messages")]
    pub n: Option<u32>,

    #[structopt(short = "-c", long = "--central")]
    pub use_central: Option<bool>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli: Cli = Cli::from_args();
    let mut producer_config_path = PathBuf::new();
    producer_config_path.push("producer-timestamp.yaml");

    let init_timestamp;
    if producer_config_path.exists() {
        let init_timestamp_reader = File::open(producer_config_path.as_path()).expect("File exists");
        let mut init_timestamp_map: BTreeMap<String, u64> = serde_yaml::from_reader(init_timestamp_reader).expect("Can parse YAML");
        init_timestamp = init_timestamp_map.remove("timestamp").expect("Did not have timestamp in config");
    } else {
        init_timestamp = 0;
    }

    let query_names_url_map = parse_names_url_map(&cli.names_url_path);
    let use_central = match cli.use_central {
        None => {false}
        Some(u) => {u}
    };

    info!("Starting from timestamp: {}", &init_timestamp);
    let mut producer = TestdataProducer::new(query_names_url_map, Some(Duration::from_secs(15)), use_central).await;
    debug!("Created producer");
    let timestamp;
    match cli.command {
        Command::SimpleFactoryConfig => {
            let simple_factory_scenario = create_simple_factory_scenario();

            let mut sfss = SimpleFactoryScenarioSimulator::new(simple_factory_scenario, init_timestamp, use_central);
            timestamp = producer.produce_until_interrupted_or_n_reached(&mut sfss, cli.messages_per_second, cli.n).await;
        },
        Command::ComplexFactoryConfig(fca) => {
            let complex_factory_scenario = complex_factory_scenario_builder(fca.size);
            let mut cfss = ComplexFactoryScenarioSimulator::new(complex_factory_scenario, init_timestamp, use_central);
            timestamp = producer.produce_until_interrupted_or_n_reached(&mut cfss, cli.messages_per_second, cli.n).await;
        }
    }

    info!("Writing timestamp: {}", &timestamp);
    let timestamp_map = BTreeMap::from([("timestamp".to_string(), timestamp)]);
    let timestamp_file = File::create(producer_config_path.as_path()).expect("Error opening file for writing");
    serde_yaml::to_writer(timestamp_file, &timestamp_map).expect("Could not write to file");
}

fn parse_names_url_map(names_url_path: &PathBuf) -> BTreeMap<String, String> {
    let names_url_map_reader = File::open(names_url_path.as_path()).expect("File open error");
    serde_yaml::from_reader(names_url_map_reader).expect("Can parse YAML")
}

