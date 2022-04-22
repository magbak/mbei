use std::collections::BTreeMap;
use std::fs::{create_dir, metadata, remove_file};
use std::path::{PathBuf};
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;
use tokio::runtime::Builder;

use mbei_central::{start_central, Central};
use mbei_component::start_component_servers;
use mbei_core::graph::Delta;
use mbei_core::query::Query;
use mbei_scenario_server::create_tonic_server;
use mbei_testdata::factory_scenario_builder::{
    crane_pickdrops, cranes, matched_pickdrop_query, matched_stamp_query, platforms,
    ramps, stamp_assemblies, stamps, SimpleFactoryScenario,
};
use mbei_testdata::producer::TestdataProducer;

pub async fn create_testdata_producer(query_url_map:BTreeMap<String, String>) -> TestdataProducer {
    TestdataProducer::new(query_url_map, Some(Duration::from_secs(10)), true).await
}

pub fn app_port() -> u16 {
    9999
}

pub fn create_app_grpc_url(port:u16) -> String {
    "http://[::1]:".to_string() + &port.to_string()
}

pub fn central_port() -> u16 {
    10000
}

pub fn create_application_grpc_server(port:u16) -> JoinHandle<()> {
    create_tonic_server(port)
}

pub fn create_query_url_map(qnames:&Vec<String>) -> BTreeMap<String, String> {
    let mut query_url_map = BTreeMap::new();
    let mut i = 1;
    for qname in qnames {
        let port = (10000 + i) as u16;
        query_url_map.insert(qname.clone(), "http://[::1]:".to_owned() + &port.to_string());
        i += 1;
    }
    query_url_map.insert("central".to_string(), "http://[::1]:".to_owned() + &central_port().to_string());
    query_url_map
}

pub fn create_query_port_map(qnames: &Vec<String>) -> BTreeMap<String, u16> {
    let mut query_port_map = BTreeMap::new();
    let mut i = 1;
    for qname in qnames {
        let port = (10000 + i) as u16;
        query_port_map.insert(qname.clone(), port);
        i += 1;
    }
    query_port_map
}

pub fn create_central(central_db_path: PathBuf, grpc_port: u16) -> JoinHandle<()> {
    let parent_dir = central_db_path.parent();
    if !metadata(parent_dir.unwrap()).is_ok() {
        create_dir(parent_dir.unwrap()).expect("Create parent dir failed");
    }
    if metadata(central_db_path.as_path()).is_ok() {
        remove_file(central_db_path.as_path()).expect("Removal failed");
    }
    let handle = thread::spawn(move || {
        start_central(central_db_path, grpc_port);
    });
    sleep(Duration::from_secs(3));
    handle
}

pub fn create_components(
    app_grpc_url: String,
    queries: &Vec<Query>,
) -> JoinHandle<()> {
    let my_query_names: Vec<String> = queries
        .iter()
        .map(|q| q.name.clone())
        .collect();
    let queries_cloned = queries.clone();
    let handle = thread::spawn(move || {
        run_components(queries_cloned, my_query_names, app_grpc_url);
    });

    sleep(Duration::from_secs(3));
    handle
}

pub fn get_all_deltas(central_db_path: PathBuf) -> Vec<Delta> {
    let testing_central = Central::new(central_db_path);
    testing_central.get_all_deltas()
}

fn run_components(queries: Vec<Query>, my_query_names:Vec<String>, application_grpc_url:String) {
    let rt = create_runtime(queries.len());
    let query_port_map = create_query_port_map(&my_query_names);
    let query_url_map = create_query_url_map(&my_query_names);
    rt.block_on(async {
        let mut handles = vec![];
        for query_name in my_query_names {
            let handle = rt.spawn(start_component_servers(queries.clone(), vec![query_name.clone()], application_grpc_url.clone(), query_port_map.get(&query_name).unwrap().clone(), query_url_map.clone(), Some(Duration::from_secs(15)), true));
            handles.push(handle);
        }
        for handle in handles {
            handle.await.expect("Problem in component");
        }
    });
}

fn create_runtime(num_components: usize) -> tokio::runtime::Runtime {
    let threads = num_components + 1; //One additional thread for following up sent messages
    let rt = Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_io()
        .enable_time()
        .build()
        .expect("Could not create runtime");
    rt
}

pub fn three_crane_scenario() -> SimpleFactoryScenario {
    let two_cranes = cranes(2);
    let two_pickdrops = crane_pickdrops(2);
    let three_platforms = platforms(3);
    let three_stamp_assemblies = stamp_assemblies(3);
    let three_stamps = stamps(3);
    let my_ramp = ramps(1).pop().unwrap();

    let matched_pickdrop_query_1 = matched_pickdrop_query(
        two_cranes.get(0).unwrap().clone(),
        two_pickdrops.get(0).unwrap().clone(),
        vec![
            three_platforms.get(0).unwrap().clone(),
            three_platforms.get(1).unwrap().clone(),
            three_stamp_assemblies.get(0).unwrap().clone(),
            three_stamp_assemblies.get(1).unwrap().clone(),
            my_ramp.clone(),
        ],
        "_matched_1",
    );

    let matched_pickdrop_query_2 = matched_pickdrop_query(
        two_cranes.get(1).unwrap().clone(),
        two_pickdrops.get(1).unwrap().clone(),
        vec![
            three_platforms.get(1).unwrap().clone(),
            three_platforms.get(2).unwrap().clone(),
            three_stamp_assemblies.get(1).unwrap().clone(),
            three_stamp_assemblies.get(2).unwrap().clone(),
            my_ramp.clone(),
        ],
        "_matched_2",
    );

    let matched_stamp_query_1 = matched_stamp_query(
        three_stamp_assemblies.get(0).unwrap().clone(),
        three_stamps.get(0).unwrap().clone(),
        "_matched_1",
    );

    let matched_stamp_query_2 = matched_stamp_query(
        three_stamp_assemblies.get(1).unwrap().clone(),
        three_stamps.get(1).unwrap().clone(),
        "_matched_2",
    );

    let matched_stamp_query_3 = matched_stamp_query(
        three_stamp_assemblies.get(2).unwrap().clone(),
        three_stamps.get(2).unwrap().clone(),
        "_matched_3",
    );

    SimpleFactoryScenario {
        cranes: two_cranes,
        crane_pickdrops: two_pickdrops,
        platforms: three_platforms,
        ramps: vec![my_ramp],
        stamps: three_stamps,
        stamp_assemblies: three_stamp_assemblies,
        queries: vec![
            matched_pickdrop_query_1,
            matched_pickdrop_query_2,
            matched_stamp_query_1,
            matched_stamp_query_2,
            matched_stamp_query_3,
        ],
    }
}
