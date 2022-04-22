use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::thread::{JoinHandle, sleep};
use std::time::Duration;

use bincode::config::Configuration;
use rstest::{fixture, rstest};
use serial_test::serial;

use mbei_core::event::Event;
#[cfg(test)]
use mbei_core::graph::{Delta, DeltaType};
use mbei_scenario_server::crane::{CraneEvent, CraneEventType};
use mbei_testdata::factory_scenario_builder::{barrels, crane_pickdrops, cranes, SimpleFactoryScenario, matched_pickdrop_query, platforms, ramps};

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
    p.push("tests/single_crane/");
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
fn config() -> Configuration {
    bincode::config::standard()
}

#[fixture]
pub fn factory_scenario() -> SimpleFactoryScenario {
    let my_crane = cranes(1).pop().unwrap();
    let my_pickdrop = crane_pickdrops(1).pop().unwrap();
    let my_ramp = ramps(1).pop().unwrap();
    let my_platform = platforms(1).pop().unwrap();

    let matched_query = matched_pickdrop_query(
        my_crane.clone(),
        my_pickdrop.clone(),
        vec![my_ramp.clone(), my_platform.clone(), my_ramp.clone()],
        "_matched");

    SimpleFactoryScenario {
        cranes: vec![my_crane],
        crane_pickdrops: vec![my_pickdrop],
        platforms: vec![my_platform],
        ramps: vec![my_ramp],
        stamps: vec![],
        stamp_assemblies: vec![],
        queries: vec![matched_query],
    }
}

#[fixture]
fn query_url_map(factory_scenario: SimpleFactoryScenario) -> BTreeMap<String, String> {
    let query_names = factory_scenario.queries.iter().map(|q|q.name.clone()).collect();
    create_query_url_map(&query_names)
}


#[fixture]
fn components(app_grpc_url: &String,
              factory_scenario: SimpleFactoryScenario) -> JoinHandle<()> {
    create_components( app_grpc_url.clone(), &factory_scenario.queries)
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_simple_delta_and_event(start_logging: (),
                                     app_grpc_server: &JoinHandle<()>,
                                     config: Configuration,
                                     components: JoinHandle<()>,
                                     central: JoinHandle<()>,
                                     query_url_map: BTreeMap<String, String>,
                                     factory_scenario: SimpleFactoryScenario,
                                     central_db_path: PathBuf) {
    let _ = (app_grpc_server, start_logging); //avoid warning in compile
    let producer = create_testdata_producer(query_url_map).await;
    let my_barrel = barrels(1).pop().unwrap();
    let my_platform = factory_scenario.platforms.get(0).unwrap();
    let my_crane = factory_scenario.cranes.get(0).unwrap();
    let my_pickdrop = factory_scenario.crane_pickdrops.get(0).unwrap();
    let my_barrel_at_my_platform = Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 1u64,
        delta_type: DeltaType::Addition,
    };
    let crane_event = CraneEvent {
        instance_node_id: my_platform.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::PickUp,
    };
    let payload = bincode::encode_to_vec(crane_event, config)
        .expect("Encodable");
    let pickup_barrel_at_platform = Event {
        event_id: "myevent".to_string(),
        timestamp: 3u64,
        node_id: my_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: payload,
    };

    producer.send_deltas_now("mydelta", "pickdrop_matched", vec![my_barrel_at_my_platform.clone()]).await;
    producer.send_event_now( "pickdrop_matched", pickup_barrel_at_platform).await;
    sleep(Duration::from_secs(5));

    producer.send_stop_now("pickdrop_matched").await;
    producer.send_stop_now("central").await;

    components.join().expect("Error joining component");
    central.join().expect("Error joining central");

    let deltas = get_all_deltas(central_db_path);
    let expected_deltas =
        vec![
            my_barrel_at_my_platform.clone(),
            Delta {
                src: my_barrel.clone(),
                trg: my_platform.clone(),
                edge_type: "At".to_string(),
                timestamp: 3,
                delta_type: DeltaType::Removal,
            },
            Delta {
                src: my_barrel.clone(),
                trg: my_crane.clone(),
                edge_type: "At".to_string(),
                timestamp: 4,
                delta_type: DeltaType::Addition,
            }];
    assert_eq!(deltas.len(), expected_deltas.len());
    assert_eq!(BTreeSet::from_iter(deltas), BTreeSet::from_iter(expected_deltas));
    sleep(Duration::from_secs(3));
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_simple_delta_and_event_and_distractor(start_logging: (),
                                                    app_grpc_server: &JoinHandle<()>,
                                                    config: Configuration,
                                                    components: JoinHandle<()>,
                                                    central: JoinHandle<()>,
                                                    query_url_map: BTreeMap<String, String>,
                                                    factory_scenario: SimpleFactoryScenario,
                                                    central_db_path: PathBuf) {
    let _ = (app_grpc_server, start_logging); //avoid warning in compile
    let producer = create_testdata_producer(query_url_map).await;

    let mut the_barrels = barrels(2);
    let my_barrel = the_barrels.pop().unwrap();
    let my_other_barrel = the_barrels.pop().unwrap();
    let my_platform = factory_scenario.platforms.get(0).unwrap();
    let my_crane = factory_scenario.cranes.get(0).unwrap();
    let my_pickdrop = factory_scenario.crane_pickdrops.get(0).unwrap();
    let my_ramp = factory_scenario.ramps.get(0).unwrap();
    let my_barrel_at_my_platform = Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 1u64,
        delta_type: DeltaType::Addition,
    };
    let my_other_barrel_at_my_ramp = Delta {
        src: my_other_barrel.clone(),
        trg: my_ramp.clone(),
        edge_type: "At".to_string(),
        timestamp: 1u64,
        delta_type: DeltaType::Addition,
    };
    let crane_event = CraneEvent {
        instance_node_id: my_platform.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::PickUp,
    };
    let payload = bincode::encode_to_vec(crane_event, config)
        .expect("Encodable");
    let pickup_barrel_at_platform = Event {
        event_id: "myevent".to_string(),
        timestamp: 3u64,
        node_id: my_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: payload,
    };

    producer.send_deltas_now("mydelta", "pickdrop_matched",
                         vec![my_barrel_at_my_platform.clone(), my_other_barrel_at_my_ramp.clone()]).await;
    producer.send_event_now( "pickdrop_matched",
                        pickup_barrel_at_platform).await;
    sleep(Duration::from_secs(5));

    producer.send_stop_now("pickdrop_matched").await;
    producer.send_stop_now("central").await;
    sleep(Duration::from_secs(1));

    components.join().expect("Error joining component");
    central.join().expect("Error joining central");

    let deltas = get_all_deltas(central_db_path);
    let expected_deltas =
        vec![
            my_barrel_at_my_platform.clone(),
            my_other_barrel_at_my_ramp.clone(),
            Delta {
                src: my_barrel.clone(),
                trg: my_platform.clone(),
                edge_type: "At".to_string(),
                timestamp: 3,
                delta_type: DeltaType::Removal,
            },
            Delta {
                src: my_barrel.clone(),
                trg: my_crane.clone(),
                edge_type: "At".to_string(),
                timestamp: 4,
                delta_type: DeltaType::Addition,
            }];
    assert_eq!(deltas.len(), expected_deltas.len());
    assert_eq!(BTreeSet::from_iter(deltas), BTreeSet::from_iter(expected_deltas));
    sleep(Duration::from_secs(3));
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_out_of_sequence_reprocessing(start_logging: (),
                                           app_grpc_server: &JoinHandle<()>,
                                           config: Configuration,
                                           components: JoinHandle<()>,
                                           central: JoinHandle<()>,
                                           query_url_map: BTreeMap<String, String>,
                                           factory_scenario: SimpleFactoryScenario,
                                           central_db_path: PathBuf) {
    let _ = (app_grpc_server, start_logging); //avoid warning in compile
    let producer = create_testdata_producer(query_url_map).await;

    let mut the_barrels = barrels(2);
    let my_barrel = the_barrels.pop().unwrap();
    let my_other_barrel = the_barrels.pop().unwrap();
    let my_platform = factory_scenario.platforms.get(0).unwrap();
    let my_crane = factory_scenario.cranes.get(0).unwrap();
    let my_pickdrop = factory_scenario.crane_pickdrops.get(0).unwrap();
    let my_barrel_at_my_platform = Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 2u64,
        delta_type: DeltaType::Addition,
    };
    let my_other_barrel_at_my_platform = Delta {
        src: my_other_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 0u64,
        delta_type: DeltaType::Addition,
    };
    let crane_event = CraneEvent {
        instance_node_id: my_platform.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::PickUp,
    };
    let payload = bincode::encode_to_vec(crane_event, config)
        .expect("Encodable");
    let pickup_barrel_at_platform = Event {
        event_id: "myevent".to_string(),
        timestamp: 1u64,
        node_id: my_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: payload,
    };

    producer.send_deltas_now("mydelta", "pickdrop_matched",
                         vec![my_barrel_at_my_platform.clone()]).await;
    producer.send_event_now( "pickdrop_matched",
                        pickup_barrel_at_platform).await;
    producer.send_deltas_now("my_other_delta", "pickdrop_matched",
                         vec![my_other_barrel_at_my_platform.clone()]).await;
    sleep(Duration::from_secs(5));

    producer.send_stop_now("pickdrop_matched").await;
    producer.send_stop_now("central").await;

    components.join().expect("Error joining component");
    central.join().expect("Error joining central");

    let deltas = get_all_deltas(central_db_path);
    let expected_deltas =
        vec![
            my_barrel_at_my_platform.clone(),
            my_other_barrel_at_my_platform.clone(),
            Delta {
                src: my_other_barrel.clone(),
                trg: my_platform.clone(),
                edge_type: "At".to_string(),
                timestamp: 1,
                delta_type: DeltaType::Removal,
            },
            Delta {
                src: my_other_barrel.clone(),
                trg: my_crane.clone(),
                edge_type: "At".to_string(),
                timestamp: 2,
                delta_type: DeltaType::Addition,
            }];
    assert_eq!(deltas.len(), expected_deltas.len());
    assert_eq!(BTreeSet::from_iter(deltas), BTreeSet::from_iter(expected_deltas));
    sleep(Duration::from_secs(3));
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_out_of_sequence_retraction(start_logging: (),
                                         app_grpc_server: &JoinHandle<()>,
                                         config: Configuration,
                                         components: JoinHandle<()>,
                                         central: JoinHandle<()>,
                                         query_url_map: BTreeMap<String, String>,
                                         factory_scenario: SimpleFactoryScenario,
                                         central_db_path: PathBuf) {
    sleep(Duration::from_secs(5));
    let _ = (app_grpc_server, start_logging); //avoid warning in compile
    let producer = create_testdata_producer(query_url_map).await;
    let mut the_barrels = barrels(1);
    let my_barrel = the_barrels.pop().unwrap();
    let my_platform = factory_scenario.platforms.get(0).unwrap();
    let my_pickdrop = factory_scenario.crane_pickdrops.get(0).unwrap();
    let my_barrel_at_my_platform = Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 0u64,
        delta_type: DeltaType::Addition,
    };

    let crane_event = CraneEvent {
        instance_node_id: my_platform.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::PickUp,
    };
    let payload = bincode::encode_to_vec(crane_event, config)
        .expect("Encodable");
    let pickup_barrel_at_platform = Event {
        event_id: "myevent".to_string(),
        timestamp: 3u64,
        node_id: my_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: payload,
    };

    let my_barrel_not_at_my_platform = Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 2u64,
        delta_type: DeltaType::Removal,
    };

    producer.send_deltas_now("mydelta", "pickdrop_matched",
                         vec![my_barrel_at_my_platform.clone()]).await;
    producer.send_event_now( "pickdrop_matched",
                        pickup_barrel_at_platform).await;
    sleep(Duration::from_secs(1));
    producer.send_deltas_now("my_other_delta", "pickdrop_matched",
                         vec![my_barrel_not_at_my_platform.clone()]).await;
    sleep(Duration::from_secs(5));

    producer.send_stop_now("pickdrop_matched").await;
    producer.send_stop_now("central").await;

    components.join().expect("Error joining component");
    central.join().expect("Error joining central");

    let deltas = get_all_deltas(central_db_path);
    let expected_deltas =
        vec![
            my_barrel_at_my_platform.clone(),
            my_barrel_not_at_my_platform.clone()
            ];
    assert_eq!(deltas.len(), expected_deltas.len());
    assert_eq!(BTreeSet::from_iter(deltas), BTreeSet::from_iter(expected_deltas));
    sleep(Duration::from_secs(3));
}


#[rstest]
#[tokio::test]
#[serial]
async fn test_back_and_forth(start_logging: (),
                             app_grpc_server: &JoinHandle<()>,
                             config: Configuration,
                             components: JoinHandle<()>,
                             central: JoinHandle<()>,
                             query_url_map: BTreeMap<String, String>,
                             factory_scenario: SimpleFactoryScenario,
                             central_db_path: PathBuf) {
    let _ = (app_grpc_server, start_logging); //avoid warning in compile
    let producer = create_testdata_producer(query_url_map).await;

    let my_barrel = barrels(1).pop().unwrap();
    let my_platform = factory_scenario.platforms.get(0).unwrap();
    let my_crane = factory_scenario.cranes.get(0).unwrap();
    let my_pickdrop = factory_scenario.crane_pickdrops.get(0).unwrap();
    let my_barrel_at_my_platform = Delta {
        src: my_barrel.clone(),
        trg: my_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 1u64,
        delta_type: DeltaType::Addition,
    };
    let crane_event_pickup = CraneEvent {
        instance_node_id: my_platform.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::PickUp,
    };
    let payload_pickup = bincode::encode_to_vec(crane_event_pickup, config)
        .expect("Encodable");
    let pickup_barrel_at_platform = Event {
        event_id: "my_first_event".to_string(),
        timestamp: 3u64,
        node_id: my_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: payload_pickup,
    };

    let crane_event_drop = CraneEvent {
        instance_node_id: my_platform.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::Drop,
    };
    let payload_drop = bincode::encode_to_vec(crane_event_drop, config)
        .expect("Encodable");
    let drop_barrel_at_platform = Event {
        event_id: "my_second_event".to_string(),
        timestamp: 5u64,
        node_id: my_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: payload_drop,
    };

    producer.send_deltas_now("mydelta", "pickdrop_matched",
                         vec![my_barrel_at_my_platform.clone()]).await;
    producer.send_event_now( "pickdrop_matched",
                        pickup_barrel_at_platform).await;
    producer.send_event_now("pickdrop_matched",
                        drop_barrel_at_platform).await;
    sleep(Duration::from_secs(5));

    producer.send_stop_now("pickdrop_matched").await;
    producer.send_stop_now("central").await;

    components.join().expect("Error joining component");
    central.join().expect("Error joining central");

    let deltas = get_all_deltas(central_db_path);
    let expected_deltas =
        vec![
            my_barrel_at_my_platform.clone(),
            Delta {
                src: my_barrel.clone(),
                trg: my_platform.clone(),
                edge_type: "At".to_string(),
                timestamp: 3,
                delta_type: DeltaType::Removal,
            },
            Delta {
                src: my_barrel.clone(),
                trg: my_crane.clone(),
                edge_type: "At".to_string(),
                timestamp: 4,
                delta_type: DeltaType::Addition,
            },
            Delta {
                src: my_barrel.clone(),
                trg: my_crane.clone(),
                edge_type: "At".to_string(),
                timestamp: 5,
                delta_type: DeltaType::Removal,
            },
            Delta { src: my_barrel.clone(), trg: my_platform.clone(), edge_type: "At".to_string(), timestamp: 6, delta_type: DeltaType::Addition },
        ];
    assert_eq!(deltas.len(), expected_deltas.len());
    assert_eq!(BTreeSet::from_iter(deltas), BTreeSet::from_iter(expected_deltas));
    sleep(Duration::from_secs(3));
}