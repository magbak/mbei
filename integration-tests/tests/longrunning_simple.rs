use std::collections::BTreeMap;
use std::path::PathBuf;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;
use mbei_testdata::message_creator::MessageCreator;

use mbei_core::graph::Delta;
use rstest::{fixture, rstest};
use serial_test::serial;

use mbei_testdata::factory_scenario_builder::SimpleFactoryScenario;
use mbei_testdata::simple_factory_scenario::SimpleFactoryScenarioSimulator;

use crate::common::{app_port, central_port, create_app_grpc_url, create_application_grpc_server, create_central, create_components, create_query_url_map, create_testdata_producer, get_all_deltas, three_crane_scenario};

#[cfg(test)]
mod common;

#[fixture]
#[once]
pub fn start_logging() {
    env_logger::init();
}

#[fixture]
pub fn testdata_path() -> PathBuf {
    let _ = start_logging;
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/longrunning/");
    p
}

#[fixture]
#[once]
pub fn app_grpc_url() -> String { create_app_grpc_url(app_port()) }

#[fixture]
#[once]
fn app_grpc_server() -> JoinHandle<()> {
    create_application_grpc_server(app_port())
}

#[fixture]
fn central_db_path(testdata_path: PathBuf) -> PathBuf {
    let mut sqlite_db_path = testdata_path.clone();
    sqlite_db_path.push("central.db");
    sqlite_db_path
}

#[fixture]
fn central(central_db_path: PathBuf) -> JoinHandle<()> {
    create_central(central_db_path, central_port())
}

#[fixture]
fn query_url_map(factory_scenario: SimpleFactoryScenario) -> BTreeMap<String, String> {
    let query_names = factory_scenario.queries.iter().map(|q|q.name.clone()).collect();
    create_query_url_map(&query_names)
}

#[fixture]
pub fn factory_scenario() -> SimpleFactoryScenario {
    three_crane_scenario()
}


#[fixture]
fn components(
    app_grpc_url: &String,
    factory_scenario: SimpleFactoryScenario,
) -> JoinHandle<()> {
    create_components(app_grpc_url.clone(), &factory_scenario.queries)
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_stress_100_iterations_simple(
    start_logging: (),
    app_grpc_server: &JoinHandle<()>,
    components: JoinHandle<()>,
    central: JoinHandle<()>,
    query_url_map: BTreeMap<String, String>,
    factory_scenario: SimpleFactoryScenario,
    central_db_path: PathBuf,
) {
    sleep(Duration::from_secs(1));
    let _ = (start_logging, app_grpc_server);
    let producer = create_testdata_producer(query_url_map).await;
    let mut factory_scenario_producer = SimpleFactoryScenarioSimulator::new(factory_scenario.clone(), 0, true);


    let mut expected_deltas: Vec<Delta> = vec![];
    for _ in 0..100 {
        let (mut deltas, tus) = factory_scenario_producer
            .create_one_new_message(false);
        let mut message_handles = vec![];
        for tu in tus {
            message_handles.push(producer.send_update(&tu.update, tu.topic_name).await);
        }
        expected_deltas.append(&mut deltas);
        for m in message_handles {
            m.await.expect("Producer error").expect("Producer error");
        }
    }

    sleep(Duration::from_secs(15));

    for q in &factory_scenario.queries {
        producer
            .send_stop_now(q.name.as_str())
            .await;
    }
    producer
        .send_stop_now("central")
        .await;
    components.join().expect("Error joining components");
    central.join().expect("Error joining central");

    let mut actual_deltas = get_all_deltas(central_db_path);
    expected_deltas.sort_by_key(|d| d.timestamp);
    actual_deltas.sort_by_key(|d| d.timestamp);
    assert_eq!(actual_deltas, expected_deltas);
}
