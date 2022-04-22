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
use log::info;

use structopt::StructOpt;
use mbei_testdata::producer::{TestdataProducer, TopicNameAndUpdate};

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(short = "-u", long = "--names-url-path", parse(from_os_str))]
    pub names_url_path: PathBuf,

    #[structopt(short = "-c", long = "--central")]
    pub use_central: Option<bool>,

    #[structopt(short = "-i", long = "--input-file", parse(from_os_str))]
    pub input_file: PathBuf,

    #[structopt(short = "-r", long = "--reverse")]
    pub reverse: Option<bool>,

    #[structopt(short = "-l", long = "--latency")]
    pub latency: Option<bool>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli: Cli = Cli::from_args();
    let input_reader = File::open(cli.input_file.as_path()).expect("File exists");
    let messages: Vec<TopicNameAndUpdate> = serde_yaml::from_reader(input_reader).expect("Error parsing messages");

    let query_names_url_map = parse_names_url_map(&cli.names_url_path);
    let use_central = match cli.use_central {
        None => {false}
        Some(u) => {u}
    };

    info!("Starting producer");
    let producer = TestdataProducer::new(query_names_url_map, Some(Duration::from_secs(15)), use_central).await;
    info!("Finished starting producer");
    info!("Started sending messages");
    let reverse = match cli.reverse {
        None => {false}
        Some(t) => {t}
    };
    let latency = match cli.latency {
        None => {false},
        Some(t) => {t}
    };
    if !reverse && !latency {
        info!("Sending messages in normal order");
        producer.send_messages_in_vec_until_max_queue(messages, 20, 100, 10, 1000).await;
    }
    else if reverse {
        info!("Sending messages in reversed order");
        producer.send_reversed_messages_in_vec_until_max_queue(messages, 20, 100, 10, 1000).await;
    }
    else {
        info!("Sending messages in fixed rate to measure latency");
        producer.send_messages_in_vec_until_max_queue(messages, 20, 300, 0, 1000000).await;
    }
    info!("Finished sending messages");
}

fn parse_names_url_map(names_url_path: &PathBuf) -> BTreeMap<String, String> {
    let names_url_map_reader = File::open(names_url_path.as_path()).expect("File open error");
    serde_yaml::from_reader(names_url_map_reader).expect("Can parse YAML")
}


