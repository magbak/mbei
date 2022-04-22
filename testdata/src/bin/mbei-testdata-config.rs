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

use log::error;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::PathBuf;
use bincode::config::Configuration;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{RngCore, SeedableRng};

use mbei_core::query::Query;
use mbei_testdata::factory_scenario_builder::{
    complex_factory_scenario_builder, create_simple_factory_scenario,
};
use structopt::StructOpt;

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

    #[structopt(short = "-p", long = "--path", parse(from_os_str))]
    pub output_path: std::path::PathBuf,

    #[structopt(short = "-o", long = "--overwrite")]
    pub overwrite: bool,

    #[structopt(short = "-n", long = "--nodes")]
    pub nodes: u32,
}

fn main() {
    env_logger::init();
    let cli: Cli = Cli::from_args();

    match cli.command {
        Command::SimpleFactoryConfig => {
            let simple_factory_scenario = create_simple_factory_scenario();
            write_scenario_to_path(
                &cli.output_path,
                simple_factory_scenario.queries,
                cli.overwrite,
                cli.nodes
            );
        }
        Command::ComplexFactoryConfig(fca) => {
            let complex_factory_scenario = complex_factory_scenario_builder(fca.size);
            let all_queries = complex_factory_scenario.all_queries();
            write_scenario_to_path(&cli.output_path, all_queries, cli.overwrite, cli.nodes);
        }
    }
}

fn write_scenario_to_path(
    output_path: &PathBuf,
    queries: Vec<Query>,
    overwrite_files: bool,
    nodes: u32,
) {
    let ouput_path = output_path.clone();
    if !ouput_path.exists() || !ouput_path.is_dir() {
        error!(
            "Output path {:?} does not exist or is not a directory",
            &ouput_path
        );
        panic!();
    }
    let mut query_names: Vec<String> = queries.iter().map(|q| q.name.clone()).collect();
    query_names.sort();

    let config = bincode::config::standard();
    write_all_queries(queries.clone(), ouput_path.clone(), overwrite_files, config);

    let assignment = create_assignment(query_names.clone(), nodes);
    write_assignments(assignment.clone(), ouput_path.clone(), overwrite_files);

    let names_url_map = create_names_url_map(assignment);
    write_names_url_map(names_url_map, output_path.clone(), overwrite_files);
}

fn check_if_file_exists_and_possibly_overwrite(p: &PathBuf, overwrite: bool) {
    if p.exists() && !overwrite {
        error!(
            "File  {:?} already exists, try running with -o/--overwrite ",
            &p
        );
        panic!();
    }
}

fn create_assignment(mut query_names:Vec<String>, nodes:u32) -> BTreeMap<u32, Vec<String>> {
    let mut generator: ChaCha8Rng = rand_chacha::ChaCha8Rng::seed_from_u64(1234);
    let mut assignment = BTreeMap::from_iter((0..nodes).map(|i|(i, vec![])));
    'outer: loop {
        for n in 0..nodes {
            if !query_names.is_empty() {
                let i = (generator.next_u64() as usize) % query_names.len();
                assignment.get_mut(&n).unwrap().push(query_names.remove(i));
            } else {
                break 'outer;
            }
        }
    }
    assignment
}

fn create_names_url_map(assignment: BTreeMap<u32, Vec<String>>) -> BTreeMap<String, String> {
    let mut names_url_map = BTreeMap::new();
    for (k, qs) in assignment {
        let mut i = 0;
        for q in qs {
            let port = 10000 + i;
            let url = format!("http://mbei-component-{}.mbei-component.mbei.svc.cluster.local:{}", k, port);
            names_url_map.insert(q, url.clone());
            i += 1;
        }
    }
    names_url_map
}

fn write_all_queries(queries:Vec<Query>, output_path:PathBuf, overwrite_files:bool, config:Configuration) {
    let mut queries_path = output_path.clone();
    queries_path.push("all-queries.yaml");
    check_if_file_exists_and_possibly_overwrite(&queries_path, overwrite_files);
    let queries_file =
        File::create(queries_path.as_path()).expect("Error opening file for writing");
    let mut out = HashMap::new();
    for q in queries {
        out.insert(q.name.clone(), q.to_bytestring(config));
    }
    serde_yaml::to_writer(queries_file, &out).expect("Error writing to disk");
}

fn write_assignments(assignment: BTreeMap<u32, Vec<String>>, output_path:PathBuf, overwrite_files:bool) {
    let mut assignments_path = output_path.clone();
    assignments_path.push("query-assignments.yaml");
    check_if_file_exists_and_possibly_overwrite(&assignments_path, overwrite_files);
    let assignment_file =
        File::create(assignments_path.as_path()).expect("Error opening file for writing");
    serde_yaml::to_writer(assignment_file, &assignment).expect("Error writing to disk");
}

fn write_names_url_map(names_url_map: BTreeMap<String, String>, output_path:PathBuf, overwrite_files:bool) {
    let mut names_url_map_path = output_path.clone();
    names_url_map_path.push("names-url-map.yaml");
    check_if_file_exists_and_possibly_overwrite(&names_url_map_path, overwrite_files);
    let names_url_map_file =
        File::create(names_url_map_path.as_path()).expect("Error opening file for writing");
    serde_yaml::to_writer(names_url_map_file, &names_url_map).expect("Error writing to disk");
}