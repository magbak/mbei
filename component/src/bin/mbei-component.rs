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

use log::{debug, error, info};
use std::collections::{BTreeMap};
use std::fs::File;
use std::path::PathBuf;

use structopt::StructOpt;

use mbei_component::{start_component_servers};
use mbei_core::query::{parse_queries, Query};

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(short = "-q", long = "--queries-path", parse(from_os_str))]
    pub queries_path: std::path::PathBuf,

    #[structopt(short = "-a", long = "--assignments-path", parse(from_os_str))]
    pub assignments_path: std::path::PathBuf,

    #[structopt(short = "-u", long = "--url-map-path", parse(from_os_str))]
    pub url_map_path: std::path::PathBuf,

    #[structopt(short = "-m", long = "--module_grpc_url")]
    pub module_grpc_url: String,

    #[structopt(short = "-p", long = "--port")]
    pub port: u16,

    #[structopt(short = "-n", long = "--host_number")]
    pub host_number: Option<u16>,

    #[structopt(short = "-c", long = "--central")]
    pub use_central: Option<bool>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli: Cli = Cli::from_args();

    let host_number;
    if cli.host_number.is_some() {
        host_number = cli.host_number.unwrap()
    } else {
        let hostname = hostname::get().expect("Could not find hostname");
        let hostname = hostname
            .into_string()
            .expect("Could not convert hostname to string");
        debug!("Hostname is: {}", hostname);
        let errmsg = "Invalid hostname, expected mbei-N, where N is a number";
        host_number = hostname
            .split("-")
            .last()
            .expect(errmsg)
            .parse()
            .expect(errmsg);
    }
    info!("Host number is: {:?}", &host_number);

    let config = bincode::config::standard();
    let queries: Vec<Query> = parse_queries(&cli.queries_path, config);
    let query_url_map: BTreeMap<String, String> = parse_names_url_map(&cli.url_map_path);
    let query_names_map = parse_query_names_map(&cli.assignments_path);

    let query_names;
    if query_names_map.contains_key(&host_number) {
        query_names = query_names_map.get(&host_number).unwrap().clone();
    } else {
        error!("Host number {:?} not found in map {:?}", &host_number, &query_names_map);
        panic!("Unable to find query name in map");
    }

    let use_central = match cli.use_central {
        None => {false}
        Some(u) => {u}
    };

    start_component_servers(queries, query_names,cli.module_grpc_url, cli.port, query_url_map, None, use_central).await;
}

fn parse_names_url_map(names_url_path: &PathBuf) -> BTreeMap<String, String> {
    let names_url_map_reader = File::open(names_url_path.as_path()).expect("File open error");
    serde_yaml::from_reader(names_url_map_reader).expect("Can parse YAML")
}

fn parse_query_names_map(query_names_path: &PathBuf) -> BTreeMap<u16, Vec<String>> {
    let query_name_map_reader = File::open(query_names_path.as_path()).expect("File open error");
    let query_name_map = serde_yaml::from_reader(query_name_map_reader).expect("Can not parse YAML");
    query_name_map
}
