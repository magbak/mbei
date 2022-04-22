use std::collections::BTreeMap;
use std::path::PathBuf;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use mbei_core::graph::Delta;
use rstest::{fixture, rstest};
use serial_test::serial;

use mbei_testdata::factory_scenario_builder::{complex_factory_scenario_builder, ComplexFactoryScenario};
use mbei_testdata::complex_factory_scenario::ComplexFactoryScenarioSimulator;
use mbei_testdata::message_creator::MessageCreator;

use crate::common::{app_port, central_port, create_app_grpc_url, create_application_grpc_server, create_central, create_components, create_query_url_map, create_testdata_producer, get_all_deltas};

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
pub fn factory_scenario() -> ComplexFactoryScenario { complex_factory_scenario_builder(2) }

#[fixture]
fn query_url_map(factory_scenario: ComplexFactoryScenario) -> BTreeMap<String, String> {
    let query_names = factory_scenario.all_queries().iter().map(|q|q.name.clone()).collect();
    create_query_url_map(&query_names)
}

#[fixture]
fn components(
    app_grpc_url: &String,
    factory_scenario: ComplexFactoryScenario,
) -> JoinHandle<()> {
    let all_queries = factory_scenario.all_queries();
    create_components(app_grpc_url.clone(), &all_queries)
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_stress_20_iterations_complex(
    start_logging: (),
    app_grpc_server: &JoinHandle<()>,
    components: JoinHandle<()>,
    central: JoinHandle<()>,
    query_url_map: BTreeMap<String, String>,
    factory_scenario: ComplexFactoryScenario,
    central_db_path: PathBuf,
) {
    sleep(Duration::from_secs(1));
    let _ = (start_logging, app_grpc_server);
    let producer = create_testdata_producer(query_url_map).await;
    let mut factory_scenario_producer = ComplexFactoryScenarioSimulator::new( factory_scenario.clone(), 0, true);

    let mut expected_deltas: Vec<Delta> = vec![];
    for _ in 0..20 {
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

    let all_queries = factory_scenario.all_queries();
    for q in &all_queries {
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

#[ignore]
#[rstest]
#[tokio::test]
#[serial]
async fn test_throttled_1000_iterations_complex(
    start_logging: (),
    app_grpc_server: &JoinHandle<()>,
    components: JoinHandle<()>,
    central: JoinHandle<()>,
    query_url_map: BTreeMap<String, String>,
    factory_scenario: ComplexFactoryScenario,
    central_db_path: PathBuf,
) {
    sleep(Duration::from_secs(1));
    let _ = (start_logging, app_grpc_server);
    let producer = create_testdata_producer(query_url_map).await;
    let mut factory_scenario_producer = ComplexFactoryScenarioSimulator::new( factory_scenario.clone(), 0, true);

    let mut expected_deltas: Vec<Delta> = vec![];
    for _ in 0..1000 {
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
        sleep(Duration::from_millis(50));
    }

    sleep(Duration::from_secs(15));

    let all_queries = factory_scenario.all_queries();
    for q in &all_queries {
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