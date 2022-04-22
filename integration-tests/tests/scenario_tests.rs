use std::collections::{BTreeMap, BTreeSet};
use mbei_component::router::Router;
use crate::common::three_crane_scenario;
use mbei_core::graph::{Edge, Graph, Node, NodeClass};
use mbei_testdata::factory_scenario_builder::complex_factory_scenario_builder;

#[cfg(test)]
mod common;

#[test]
fn test_three_crane_scenario() {
    let scenario = three_crane_scenario();

    let pickdrop_query1 = scenario
        .queries
        .iter()
        .find(|x| x.name == "pickdrop_matched_1")
        .unwrap();

    let gr = Graph::from_edges(vec![
        Edge {
            src: Node {
                query_node_name: None,
                instance_node_name: Some(
                    "MyBarrel-4ec67d47-7201-4561-bcb8-5410fad3371a".to_string(),
                ),
                node_type: Some("Barrel".to_string()),
                node_class: NodeClass::Material,
                value_bytes: None,
            },
            trg: Node {
                query_node_name: None,
                instance_node_name: Some("MyCrane0".to_string()),
                node_type: Some("Crane".to_string()),
                node_class: NodeClass::Object,
                value_bytes: None,
            },
            edge_type: "At".to_string(),
            from_timestamp: Some(121),
            to_timestamp: None,
        },
        Edge {
            src: Node {
                query_node_name: None,
                instance_node_name: Some(
                    "MyBarrel-8f450af3-dcb8-4781-a153-83788c1cef82".to_string(),
                ),
                node_type: Some("Barrel".to_string()),
                node_class: NodeClass::Material,
                value_bytes: None,
            },
            trg: Node {
                query_node_name: None,
                instance_node_name: Some("MyPlatform1".to_string()),
                node_type: Some("Platform".to_string()),
                node_class: NodeClass::Object,
                value_bytes: None,
            },
            edge_type: "At".to_string(),
            from_timestamp: Some(75),
            to_timestamp: None,
        },
        Edge {
            src: Node {
                query_node_name: None,
                instance_node_name: Some(
                    "MyBarrel-f0e3e747-7222-4970-8bb4-5d8568a198bc".to_string(),
                ),
                node_type: Some("Barrel".to_string()),
                node_class: NodeClass::Material,
                value_bytes: None,
            },
            trg: Node {
                query_node_name: None,
                instance_node_name: Some("MyPlatform1".to_string()),
                node_type: Some("Platform".to_string()),
                node_class: NodeClass::Object,
                value_bytes: None,
            },
            edge_type: "At".to_string(),
            from_timestamp: Some(30),
            to_timestamp: None,
        },
    ]);

    let matches = pickdrop_query1.find_all_grouped_matches(&gr);
    assert_eq!(matches.len(), 1);
}


#[test]
fn test_edge_forward() {
    let scenario = complex_factory_scenario_builder(4);
    let mut all_queries_by_name = BTreeMap::new();
    for q in scenario.all_queries() {
        all_queries_by_name.insert(q.name.clone(), q);
    }
    let edge_forward = Router::compute_edge_forward_maps(&all_queries_by_name);
    let edge_forward_closure = Router::compute_edge_forward_closure(edge_forward, &all_queries_by_name);

    let reachable = Router::compute_reachable_map(&edge_forward_closure);
    let all_conveyor_names = BTreeSet::from(["conveyor_matched_0".to_string(), "conveyor_matched_1".to_string()]);
    assert_eq!(reachable.get("detector_matched_0").unwrap(), &all_conveyor_names);
    assert_eq!(reachable.get("detector_matched_1").unwrap(), &all_conveyor_names);
    assert_eq!(reachable.get("detector_matched_2").unwrap(), &all_conveyor_names);
    assert_eq!(reachable.get("detector_matched_3").unwrap(), &all_conveyor_names);
}

#[test]
fn test_conveyor_matching() {
    let scenario = complex_factory_scenario_builder(2);
    let edges = vec![
        Edge { src: Node { query_node_name: None, instance_node_name: Some("MyBarrel-4d23214e-86d8-4c69-9b7e-65925c3190e3".to_string()), node_type: Some("Barrel".to_string()), node_class: NodeClass::Material, value_bytes: None }, trg: Node { query_node_name: None, instance_node_name: Some("MyConveyor0".to_string()), node_type: Some("Conveyor".to_string()), node_class: NodeClass::Object, value_bytes: None }, edge_type: "At".to_string(), from_timestamp: Some(28), to_timestamp: None },
        Edge { src: Node { query_node_name: None, instance_node_name: Some("MyBarrel-4d23214e-86d8-4c69-9b7e-65925c3190e3".to_string()), node_type: Some("Barrel".to_string()), node_class: NodeClass::Material, value_bytes: None }, trg: Node { query_node_name: None, instance_node_name: Some("MyBarrel-4d23214e-86d8-4c69-9b7e-65925c3190e3_barrel_type".to_string()), node_type: Some("BarrelMaterialType".to_string()), node_class: NodeClass::Property, value_bytes: Some(vec![1]) }, edge_type: "HasMaterialType".to_string(), from_timestamp: Some(4), to_timestamp: None }];

    let graph = Graph::from_edges(edges);
    let query = scenario.conveyor_queries.get(0).unwrap();
    let matches = query.find_all_grouped_matches(&graph);
    assert_eq!(matches.len(), 1);
}