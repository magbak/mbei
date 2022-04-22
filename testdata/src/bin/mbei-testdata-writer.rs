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

use std::fs::File;
use std::path::PathBuf;
use log::{info};

use mbei_testdata::factory_scenario_builder::{complex_factory_scenario_builder};
use mbei_testdata::producer::{TopicNameAndUpdate};
use structopt::StructOpt;
use mbei_testdata::complex_factory_scenario::ComplexFactoryScenarioSimulator;
use mbei_testdata::message_creator::MessageCreator;

#[derive(StructOpt)]
pub struct ComplexFactoryCommandArgs {
    #[structopt(short = "-s", long = "--size")]
    pub size: u32,
}

#[derive(StructOpt)]
pub enum Command {
    #[structopt(name = "complex-factory")]
    ComplexFactoryConfig(ComplexFactoryCommandArgs),
}

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(subcommand)]
    pub command: Command,

    #[structopt(short = "-n", long = "--number-of-messages")]
    pub n: u32,

    #[structopt(short = "-o", long = "--output-file", parse(from_os_str))]
    pub output_file: PathBuf,

    #[structopt(short = "-c", long = "--central")]
    pub use_central: Option<bool>,

}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli: Cli = Cli::from_args();

    let use_central = match cli.use_central {
        None => {false}
        Some(u) => {u}
    };

    let messages;
    let messages_file = File::create(cli.output_file.as_path()).expect("Error opening file for writing");
    match cli.command {
        Command::ComplexFactoryConfig(fca) => {
            let complex_factory_scenario = complex_factory_scenario_builder(fca.size);
            let mut cfss = ComplexFactoryScenarioSimulator::new(complex_factory_scenario, 0, use_central);
            messages = create_n_messages(&mut cfss, cli.n);
        }
    }
    serde_yaml::to_writer(messages_file, &messages).expect("Could not write to file");
}

fn create_n_messages(message_creator: &mut dyn MessageCreator, n:u32) -> Vec<TopicNameAndUpdate> {
    let mut topics_and_updates = vec![];
    let mut seq = 0;
    let mut stopping = false;
    loop {
        if !stopping {
            if n <= seq {
                    stopping = true;
                    info!("Reached max seq, stopping");
            }
        }
        if stopping && message_creator.is_finished() {
            info!("All barrels are flushed");
            break;
        }
        let (_, mut new_topic_names_and_updates) = message_creator.create_one_new_message(stopping);
        topics_and_updates.append(&mut new_topic_names_and_updates);
        seq += 1;
    }
    topics_and_updates
}
