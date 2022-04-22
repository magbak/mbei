use std::collections::BTreeMap;
use std::path::PathBuf;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use bincode::config::Configuration;
use rstest::{fixture, rstest};
use serial_test::serial;

use mbei_core::event::Event;
use mbei_core::graph::{Delta, DeltaType, Node};
use mbei_scenario_server::crane::{CraneEvent, CraneEventType};
use mbei_scenario_server::stamp::StampEvent;
use mbei_testdata::factory_scenario_builder::{barrels, SimpleFactoryScenario};

use mbei_testdata::producer::TestdataProducer;

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
    p.push("tests/three_crane/");
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
    three_crane_scenario()
}


#[fixture]
fn query_url_map(factory_scenario: SimpleFactoryScenario) -> BTreeMap<String, String> {
    let query_names = factory_scenario.queries.iter().map(|q|q.name.clone()).collect();
    create_query_url_map(&query_names)
}

#[fixture]
async fn producer(query_url_map:BTreeMap<String,String>) -> TestdataProducer {
    TestdataProducer::new(query_url_map, Some(Duration::from_secs(10)), true).await
}

#[fixture]
fn components(app_grpc_url:&String,
    factory_scenario: SimpleFactoryScenario,
) -> JoinHandle<()> {
    create_components( app_grpc_url.clone(), &factory_scenario.queries)
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_one_barrel_through_process(
    start_logging: (),
    app_grpc_server: &JoinHandle<()>,
    config: Configuration,
    components: JoinHandle<()>,
    central: JoinHandle<()>,
    query_url_map: BTreeMap<String, String>,
    factory_scenario: SimpleFactoryScenario,
    central_db_path: PathBuf,
) {
    let _ = (app_grpc_server, start_logging); //avoid warning in compile
    let producer = create_testdata_producer(query_url_map).await;
    let my_barrel = barrels(1).pop().unwrap();
    let my_ramp = factory_scenario.ramps.get(0).unwrap();
    let left_pickdrop = factory_scenario.crane_pickdrops.get(0).unwrap();
    let right_pickdrop = factory_scenario.crane_pickdrops.get(1).unwrap();
    let middle_platform = factory_scenario.platforms.get(1).unwrap();
    let left_crane = factory_scenario.cranes.get(0).unwrap();
    let right_crane = factory_scenario.cranes.get(1).unwrap();
    let middle_stamp = factory_scenario.stamps.get(1).unwrap();
    let middle_stamp_assembly = factory_scenario.stamp_assemblies.get(1).unwrap();
    let my_barrel_at_middle_platform = Delta {
        src: my_barrel.clone(),
        trg: middle_platform.clone(),
        edge_type: "At".to_string(),
        timestamp: 1,
        delta_type: DeltaType::Addition,
    };
    let plat_pickup_crane_event = CraneEvent {
        instance_node_id: middle_platform.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::PickUp,
    };

    let pickup_plat_payload =
        bincode::encode_to_vec(plat_pickup_crane_event, config).expect("Encoding error");
    let pickup_barrel_at_middle_platform = Event {
        event_id: "pickup_at_midplat".to_string(),
        timestamp: 2,
        node_id: right_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: pickup_plat_payload,
    };

    let drop_middle_stamp_assembly_event = CraneEvent {
        instance_node_id: middle_stamp_assembly
            .instance_node_name
            .as_ref()
            .unwrap()
            .clone(),
        crane_event_type: CraneEventType::Drop,
    };
    let drop_middle_stamp_assembly_payload =
        bincode::encode_to_vec(drop_middle_stamp_assembly_event, config).expect("Encoding error");
    let drop_at_middle_stamp_assembly = Event {
        event_id: "drop_at_midstamp".to_string(),
        timestamp: 3,
        node_id: right_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: drop_middle_stamp_assembly_payload,
    };

    let put_stamp_payload = bincode::encode_to_vec(
        StampEvent {
            stamp_data: "MyStampData".to_string(),
        },
        config,
    )
    .expect("Encoding error");
    let put_stamp = Event {
        event_id: "stamp_at_midstamp".to_string(),
        timestamp: 4,
        node_id: middle_stamp.instance_node_name.as_ref().unwrap().clone(),
        payload: put_stamp_payload,
    };

    let stamp_pickup_crane_event = CraneEvent {
        instance_node_id: middle_stamp_assembly
            .instance_node_name
            .as_ref()
            .unwrap()
            .clone(),
        crane_event_type: CraneEventType::PickUp,
    };
    let pickup_stamp_payload =
        bincode::encode_to_vec(stamp_pickup_crane_event, config).expect("Encoding error");
    let pickup_barrel_at_stamp = Event {
        event_id: "pickup_from_midstamp".to_string(),
        timestamp: 6,
        node_id: left_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: pickup_stamp_payload,
    };

    let ramp_drop_crane_event = CraneEvent {
        instance_node_id: my_ramp.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type: CraneEventType::Drop,
    };
    let drop_ramp_payload =
        bincode::encode_to_vec(ramp_drop_crane_event, config).expect("Encoding error");
    let drop_barrel_at_ramp = Event {
        event_id: "drop_at_ramp".to_string(),
        timestamp: 8,
        node_id: left_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: drop_ramp_payload,
    };

    let left_pickdrop_name = factory_scenario.queries.get(0).unwrap().name.clone();
    let right_pickdrop_name = factory_scenario.queries.get(1).unwrap().name.clone();
    let middle_stamp_name = factory_scenario.queries.get(3).unwrap().name.clone();
    producer
        .send_deltas_now(
            "barrel_at_midplat",
            &right_pickdrop_name,
            vec![my_barrel_at_middle_platform.clone()],
        )
        .await;
    producer
        .send_event_now(&right_pickdrop_name, pickup_barrel_at_middle_platform)
        .await;
    producer
        .send_event_now(&right_pickdrop_name, drop_at_middle_stamp_assembly)
        .await;
    producer.send_event_now(&middle_stamp_name, put_stamp).await;
    producer
        .send_event_now(&left_pickdrop_name, pickup_barrel_at_stamp)
        .await;
    producer
        .send_event_now(&left_pickdrop_name, drop_barrel_at_ramp)
        .await;
    sleep(Duration::from_secs(10));

    for q in &factory_scenario.queries {
        producer.send_stop_now(q.name.as_str()).await;
    }
    producer.send_stop_now("central").await;
    components.join().expect("Error joining components");
    central.join().expect("Error joining central");

    let mut deltas = get_all_deltas(central_db_path);
    deltas.sort();
    let mut expected_deltas = vec![
        my_barrel_at_middle_platform,
        Delta {
            src: my_barrel.clone(),
            trg: middle_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 2,
            delta_type: DeltaType::Removal,
        },
        Delta {
            src: my_barrel.clone(),
            trg: right_crane.clone(),
            edge_type: "At".to_string(),
            timestamp: 3,
            delta_type: DeltaType::Addition,
        },
        Delta {
            src: my_barrel.clone(),
            trg: right_crane.clone(),
            edge_type: "At".to_string(),
            timestamp: 3,
            delta_type: DeltaType::Removal,
        },
        Delta {
            src: my_barrel.clone(),
            trg: middle_stamp_assembly.clone(),
            edge_type: "At".to_string(),
            timestamp: 4,
            delta_type: DeltaType::Addition,
        },
        Delta {
            src: my_barrel.clone(),
            trg: Node::property_instance_node(
                "MyBarrel0_Stamp_4",
                "StampData",
                vec![11, 77, 121, 83, 116, 97, 109, 112, 68, 97, 116, 97],
            ),
            edge_type: "HasStampData".to_string(),
            timestamp: 4,
            delta_type: DeltaType::Addition,
        },
        Delta {
            src: my_barrel.clone(),
            trg: middle_stamp_assembly.clone(),
            edge_type: "At".to_string(),
            timestamp: 6,
            delta_type: DeltaType::Removal,
        },
        Delta {
            src: my_barrel.clone(),
            trg: left_crane.clone(),
            edge_type: "At".to_string(),
            timestamp: 7,
            delta_type: DeltaType::Addition,
        },
        Delta {
            src: my_barrel.clone(),
            trg: left_crane.clone(),
            edge_type: "At".to_string(),
            timestamp: 8,
            delta_type: DeltaType::Removal,
        },
        Delta {
            src: my_barrel.clone(),
            trg: my_ramp.clone(),
            edge_type: "At".to_string(),
            timestamp: 9,
            delta_type: DeltaType::Addition,
        },
    ];
    expected_deltas.sort();
    assert_eq!(deltas, expected_deltas);
    //println!("{:?}", deltas);
    sleep(Duration::from_secs(3));
}
