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

use mbei_core::graph::{Edge, Graph, Node};
use mbei_core::query::{GroupedQueryMatch, Query, QueryMatch};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct SimpleFactoryScenario {
    pub cranes: Vec<Node>,
    pub crane_pickdrops: Vec<Node>,
    pub platforms: Vec<Node>,
    pub ramps: Vec<Node>,
    pub stamp_assemblies: Vec<Node>,
    pub stamps: Vec<Node>,
    pub queries: Vec<Query>,
}

#[derive(Debug, Clone)]
pub struct ComplexFactoryScenario {
    pub cranes: Vec<Node>,
    pub crane_pickdrops: Vec<Node>,
    pub platforms: Vec<Node>,
    pub detectors: Vec<Node>,
    pub conveyors: Vec<Node>,
    pub conveyor_events: Vec<Node>,
    pub plastic_ramps: Vec<Node>,
    pub metal_ramps: Vec<Node>,
    pub stamp_assemblies: Vec<Node>,
    pub stamps: Vec<Node>,
    pub pickdrop_queries: Vec<Query>,
    pub stamp_queries: Vec<Query>,
    pub detector_queries: Vec<Query>,
    pub conveyor_queries: Vec<Query>,
}

impl ComplexFactoryScenario {
    pub fn all_queries(&self) -> Vec<Query> {
        let mut all_queries = vec![];
            all_queries.append(&mut self.conveyor_queries.clone());
            all_queries.append(&mut self.stamp_queries.clone());
            all_queries.append(&mut self.detector_queries.clone());
            all_queries.append(&mut self.pickdrop_queries.clone());
        all_queries
    }
}

pub fn pickdrop_query() -> Query {
    let obj = Node::object_query_node("o", "Object");
    let obj_at_pos = Node::object_query_node("oap", "ObjectAtPosition");
    let crane = Node::object_query_node("c", "Crane");
    let pickdrop = Node::event_query_node("p", "Pickdrop");
    let barrel_obj = Node::material_query_node("bo", "Barrel");
    let barrel_crane = Node::material_query_node("bc", "Barrel");

    let edge_obj_pos = Edge::without_timestamp(obj_at_pos.clone(), obj.clone(), "HasObject");

    let edge_crane_obj =
        Edge::without_timestamp(crane.clone(), obj_at_pos.clone(), "HasObjectAtPosition");

    let edge_crane_pickdrop = Edge::without_timestamp(crane.clone(), pickdrop.clone(), "HasEvent");

    let edge_barrel_at_object = Edge::without_timestamp(barrel_obj.clone(), obj.clone(), "At");

    let edge_barrel_at_crane = Edge::without_timestamp(barrel_crane.clone(), crane.clone(), "At");

    let output_edge_barrel_at_object = edge_barrel_at_object.clone();
    let output_edge_barrel_at_crane = edge_barrel_at_crane.clone();

    let optional_edges =
        BTreeSet::from([edge_barrel_at_object.clone(), edge_barrel_at_crane.clone()]);
    let output_edges = BTreeSet::from([output_edge_barrel_at_crane, output_edge_barrel_at_object]);
    let input_nodes = BTreeSet::from([pickdrop.clone()]);
    let groupby = BTreeSet::from([crane.clone()]);
    let query_graph = Graph::from_edges(vec![
        edge_obj_pos,
        edge_crane_obj,
        edge_crane_pickdrop,
        edge_barrel_at_crane,
        edge_barrel_at_object,
    ]);

    let query = Query {
        name: "pickdrop".to_string(),
        application: "pickdrop".to_string(),
        graph: query_graph,
        optional_edges: optional_edges,
        group: groupby,
        output_edges: output_edges,
        input_nodes: input_nodes,
    };
    query
}

pub fn matched_pickdrop_query(
    my_crane: Node,
    my_pickdrop: Node,
    surrounding_objects: Vec<Node>,
    suffix: &str,
) -> Query {
    let query = pickdrop_query();

    let my_crane_has_pickdrop = edges_between_node_vectors(
        &vec![my_crane.clone()],
        &vec![my_pickdrop.clone()],
        "HasEvent",
    )
    .pop()
    .unwrap();

    let crane_objects_at_position = objects_at_position(surrounding_objects);
    let mut crane_has_objects_at_position = vec![];
    for e in &crane_objects_at_position {
        crane_has_objects_at_position.push(Edge::without_timestamp(
            my_crane.clone(),
            e.src.clone(),
            "HasObjectAtPosition",
        ));
    }
    let crane_has_pickdrop = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasEvent")
        .unwrap();
    let object_at_position = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasObject")
        .unwrap();
    let crane_has_object_at_position = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasObjectAtPosition")
        .unwrap();

    let mut matches = vec![];
    for i in 0..crane_has_objects_at_position.len() {
        let mut homomorphism = BTreeMap::new();
        homomorphism.insert(
            crane_has_pickdrop.clone(),
            Some(my_crane_has_pickdrop.clone()),
        );
        homomorphism.insert(
            crane_has_object_at_position.clone(),
            Some(crane_has_objects_at_position.get(i).unwrap().clone()),
        );
        homomorphism.insert(
            object_at_position.clone(),
            Some(crane_objects_at_position.get(i).unwrap().clone()),
        );
        for e in &query.graph.edges {
            if !homomorphism.contains_key(e) {
                homomorphism.insert(e.clone(), None);
            }
        }
        matches.push(QueryMatch { homomorphism });
    }

    let grouped_query_match = GroupedQueryMatch {
        grouped_matches: matches,
    };
    let matched_query = query.create_matched_query(grouped_query_match, suffix.to_string());
    matched_query
}

pub fn stamp_query() -> Query {
    let stamp_assembly = Node::object_query_node("sa", "StampAssembly");
    let stamp = Node::event_query_node("s", "Stamp");
    let stamp_assembly_has_stamp =
        Edge::without_timestamp(stamp_assembly.clone(), stamp.clone(), "HasEvent");
    let barrel = Node::material_query_node("b", "Barrel");
    let barrel_at_stamp_assembly =
        Edge::without_timestamp(barrel.clone(), stamp_assembly.clone(), "At");

    let output_stamp_data = Node::property_query_node("bsd", "StampData");
    let output_edge_barrel_has_stampdata =
        Edge::without_timestamp(barrel, output_stamp_data, "HasProperty");

    let optional_edges = BTreeSet::new();
    let output_edges = BTreeSet::from([output_edge_barrel_has_stampdata]);
    let input_nodes = BTreeSet::from([stamp.clone()]);
    let groupby = BTreeSet::new();
    let query_graph = Graph::from_edges(vec![stamp_assembly_has_stamp, barrel_at_stamp_assembly]);

    let query = Query {
        name: "stamp".to_string(),
        application: "stamp".to_string(),
        graph: query_graph,
        optional_edges: optional_edges,
        group: groupby,
        output_edges: output_edges,
        input_nodes: input_nodes,
    };
    query
}

pub fn matched_stamp_query(my_stamp_assembly: Node, my_stamp: Node, suffix: &str) -> Query {
    let query = stamp_query();
    let my_stamp_assembly_has_my_stamp =
        Edge::without_timestamp(my_stamp_assembly.clone(), my_stamp.clone(), "HasEvent");
    let stamp_assembly_has_stamp = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasEvent")
        .unwrap();

    let mut homomorphism = BTreeMap::new();
    homomorphism.insert(
        stamp_assembly_has_stamp.clone(),
        Some(my_stamp_assembly_has_my_stamp.clone()),
    );
    for e in &query.graph.edges {
        if !homomorphism.contains_key(e) {
            homomorphism.insert(e.clone(), None);
        }
    }

    let matched_query = query.create_matched_query(
        GroupedQueryMatch {
            grouped_matches: vec![QueryMatch { homomorphism }],
        },
        suffix.to_string(),
    );
    matched_query
}

pub fn conveyor_query() -> Query {
    let conveyor = Node::object_query_node("c", "Conveyor");
    let conveyor_event = Node::event_query_node("e", "ConveyorEvent");
    let conveyor_has_conveyor_event =
        Edge::without_timestamp(conveyor.clone(), conveyor_event.clone(), "HasEvent");
    let plastic_ramp = Node::object_query_node("p", "Ramp");
    let metal_ramp = Node::object_query_node("m", "Ramp");
    let conveyor_has_plastic_ramp =
        Edge::without_timestamp(conveyor.clone(), plastic_ramp.clone(), "HasPlasticRamp");
    let conveyor_has_metal_ramp =
        Edge::without_timestamp(conveyor.clone(), metal_ramp.clone(), "HasMetalRamp");
    let barrel = Node::material_query_node("b", "Barrel");
    let barrel_material_type = Node::property_query_node("t", "BarrelMaterialType");
    let barrel_has_material_type = Edge::without_timestamp(barrel.clone(), barrel_material_type, "HasMaterialType");
    let barrel_at_conveyor = Edge::without_timestamp(barrel.clone(), conveyor.clone(), "At");
    let barrel_at_plastic_ramp = Edge::without_timestamp(barrel.clone(), plastic_ramp, "At");
    let barrel_at_metal_ramp = Edge::without_timestamp(barrel.clone(), metal_ramp, "At");

    let optional_edges = BTreeSet::new();
    let output_edges = BTreeSet::from([barrel_at_conveyor.clone(), barrel_at_plastic_ramp, barrel_at_metal_ramp]);
    let input_nodes = BTreeSet::from([conveyor_event.clone()]);
    let groupby = BTreeSet::new();
    let query_graph = Graph::from_edges(vec![
        conveyor_has_conveyor_event,
        conveyor_has_plastic_ramp,
        barrel_at_conveyor,
        barrel_has_material_type,
        conveyor_has_metal_ramp,
    ]);

    let query = Query {
        name: "conveyor".to_string(),
        application: "conveyor".to_string(),
        graph: query_graph,
        optional_edges: optional_edges,
        group: groupby,
        output_edges: output_edges,
        input_nodes: input_nodes,
    };
    query
}

pub fn matched_conveyor_query(
    my_conveyor: Node,
    my_plastic_ramp: Node,
    my_metal_ramp: Node,
    my_conveyor_event: Node,
    suffix: &str,
) -> Query {
    let query = conveyor_query();
    let my_conveyor_has_my_plastic_ramp = Edge::without_timestamp(
        my_conveyor.clone(),
        my_plastic_ramp.clone(),
        "HasPlasticRamp",
    );
    let my_conveyor_has_my_metal_ramp =
        Edge::without_timestamp(my_conveyor.clone(), my_metal_ramp.clone(), "HasMetalRamp");
    let my_conveyor_has_conveyor_event =
        Edge::without_timestamp(my_conveyor.clone(), my_conveyor_event.clone(), "HasEvent");

    let conveyor_has_plastic_ramp = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasPlasticRamp")
        .unwrap();
    let conveyor_has_metal_ramp = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasMetalRamp")
        .unwrap();
    let conveyor_has_conveyor_event = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasEvent")
        .unwrap();

    let mut homomorphism = BTreeMap::new();
    homomorphism.insert(
        conveyor_has_plastic_ramp.clone(),
        Some(my_conveyor_has_my_plastic_ramp.clone()),
    );
    homomorphism.insert(
        conveyor_has_metal_ramp.clone(),
        Some(my_conveyor_has_my_metal_ramp.clone()),
    );
    homomorphism.insert(
        conveyor_has_conveyor_event.clone(),
        Some(my_conveyor_has_conveyor_event.clone()),
    );
    for e in &query.graph.edges {
        if !homomorphism.contains_key(e) {
            homomorphism.insert(e.clone(), None);
        }
    }

    let matched_query = query.create_matched_query(
        GroupedQueryMatch {
            grouped_matches: vec![QueryMatch { homomorphism }],
        },
        suffix.to_string(),
    );
    matched_query
}

pub fn detector_query() -> Query {
    let platform = Node::object_query_node("p", "Platform");
    let detector = Node::event_query_node("d", "Detector");
    let platform_has_detector =
        Edge::without_timestamp(platform.clone(), detector.clone(), "HasEvent");
    let barrel = Node::material_query_node("b", "Barrel");
    let barrel_at_platform = Edge::without_timestamp(barrel.clone(), platform.clone(), "At");

    let output_material_type = Node::property_query_node("bmt", "BarrelMaterialType");
    let output_edge_barrel_has_material_type =
        Edge::without_timestamp(barrel, output_material_type, "HasMaterialType");

    let optional_edges = BTreeSet::new();
    let output_edges = BTreeSet::from([output_edge_barrel_has_material_type]);
    let input_nodes = BTreeSet::from([detector.clone()]);
    let groupby = BTreeSet::new();
    let query_graph = Graph::from_edges(vec![platform_has_detector, barrel_at_platform]);

    let query = Query {
        name: "detector".to_string(),
        application: "detector".to_string(),
        graph: query_graph,
        optional_edges: optional_edges,
        group: groupby,
        output_edges: output_edges,
        input_nodes: input_nodes,
    };
    query
}

pub fn matched_detector_query(my_platform: Node, my_detector: Node, suffix: &str) -> Query {
    let query = detector_query();
    let my_platform_has_my_detector =
        Edge::without_timestamp(my_platform.clone(), my_detector.clone(), "HasEvent");
    let platform_has_detector = query
        .graph
        .edges
        .iter()
        .find(|e| e.edge_type == "HasEvent")
        .unwrap();

    let mut homomorphism = BTreeMap::new();
    homomorphism.insert(
        platform_has_detector.clone(),
        Some(my_platform_has_my_detector.clone()),
    );
    for e in &query.graph.edges {
        if !homomorphism.contains_key(e) {
            homomorphism.insert(e.clone(), None);
        }
    }

    let matched_query = query.create_matched_query(
        GroupedQueryMatch {
            grouped_matches: vec![QueryMatch { homomorphism }],
        },
        suffix.to_string(),
    );
    matched_query
}

pub fn cranes(n: u32) -> Vec<Node> {
    let mut cranes = vec![];
    for i in 0..n {
        cranes.push(Node::object_instance_node(
            &("MyCrane".to_owned() + &i.to_string()),
            "Crane",
        ));
    }
    cranes
}

pub fn crane_pickdrops(n: u32) -> Vec<Node> {
    let mut pickdrops = vec![];
    for i in 0..n {
        pickdrops.push(Node::event_instance_node(
            &("MyCrane".to_owned() + &i.to_string() + ".PickDrop"),
            "PickDrop",
        ));
    }
    pickdrops
}

pub fn barrels(n: u32) -> Vec<Node> {
    let mut barrels = vec![];
    for i in 0..n {
        barrels.push(Node::material_instance_node(
            &("MyBarrel".to_owned() + &i.to_string()),
            "Barrel",
        ));
    }
    barrels
}

pub fn stamps(n: u32) -> Vec<Node> {
    let mut stamps = vec![];
    for i in 0..n {
        stamps.push(Node::event_instance_node(
            &("MyStamp".to_owned() + &i.to_string()),
            "Stamp",
        ));
    }
    stamps
}

pub fn stamp_assemblies(n: u32) -> Vec<Node> {
    let mut stamps = vec![];
    for i in 0..n {
        stamps.push(Node::object_instance_node(
            &("MyStampAssembly".to_owned() + &i.to_string()),
            "StampAssembly",
        ));
    }
    stamps
}

pub fn platforms(n: u32) -> Vec<Node> {
    let mut platforms = vec![];
    for i in 0..n {
        platforms.push(Node::object_instance_node(
            &("MyPlatform".to_owned() + &i.to_string()),
            "Platform",
        ));
    }
    platforms
}

pub fn detectors(n: u32) -> Vec<Node> {
    let mut detectors = vec![];
    for i in 0..n {
        detectors.push(Node::event_instance_node(
            &("MyDetector".to_owned() + &i.to_string()),
            "Detector",
        ));
    }
    detectors
}

pub fn conveyors(n: u32) -> Vec<Node> {
    let mut conveyors = vec![];
    for i in 0..n {
        conveyors.push(Node::object_instance_node(
            &("MyConveyor".to_owned() + &i.to_string()),
            "Conveyor",
        ));
    }
    conveyors
}

pub fn conveyor_events(n: u32) -> Vec<Node> {
    let mut conveyor_events = vec![];
    for i in 0..n {
        conveyor_events.push(Node::object_instance_node(
            &("MyConveyorEvent".to_owned() + &i.to_string()),
            "ConveyorEvent",
        ));
    }
    conveyor_events
}

pub fn ramps(n: u32) -> Vec<Node> {
    let mut ramps = vec![];
    for i in 0..n {
        ramps.push(Node::object_instance_node(
            &("MyRamp".to_owned() + &i.to_string()),
            "Ramp",
        ));
    }
    ramps
}

pub fn edges_between_node_vectors(
    sources: &Vec<Node>,
    targets: &Vec<Node>,
    edge_type: &str,
) -> Vec<Edge> {
    assert!(sources.len() == targets.len());
    let mut edges = vec![];
    for i in 0..sources.len() {
        edges.push(Edge::without_timestamp(
            sources.get(i).unwrap().clone(),
            targets.get(i).unwrap().clone(),
            &edge_type,
        ))
    }
    edges
}

pub fn objects_at_position(objects: Vec<Node>) -> Vec<Edge> {
    let mut edges = vec![];
    for o in &objects {
        let at_posn_node = Node::object_instance_node(
            &(o.instance_node_name.as_ref().unwrap().clone() + ".ObjectAtPosition"),
            "ObjectAtPosition",
        );
        edges.push(Edge {
            src: at_posn_node,
            trg: o.clone(),
            edge_type: "HasObject".to_string(),
            from_timestamp: None,
            to_timestamp: None,
        });
    }
    edges
}

pub fn create_simple_factory_scenario() -> SimpleFactoryScenario {
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

pub fn complex_factory_scenario_builder(size: u32) -> ComplexFactoryScenario {
    assert!(size >= 2);
    assert_eq!(size % 2, 0);
    let some_cranes = cranes(size);
    let some_crane_events = crane_pickdrops(size);
    let some_platforms = platforms(size);
    let some_detectors = detectors(size);
    let some_stamp_assemblies = stamp_assemblies(size - 1);
    let some_stamps = stamps(size - 1);
    let some_conveyors = conveyors(size / 2);
    let some_conveyor_events = conveyor_events(size / 2);
    let all_ramps = ramps(size);
    let some_metal_ramps: Vec<Node> = all_ramps[0..((size/2) as usize)].iter().map(|r|r.clone()).collect();
    let some_plastic_ramps: Vec<Node> = all_ramps[((size/2) as usize)..all_ramps.len()].iter().map(|r|r.clone()).collect();

    let mut matched_crane_queries = vec![];
    for i in 0..size {
        let mut surrounding = vec![];
        if i > 0 {
            surrounding.push(some_platforms.get((i - 1) as usize).unwrap().clone());
            surrounding.push(some_stamp_assemblies.get((i - 1) as usize).unwrap().clone());
        }
        surrounding.push(some_conveyors.get((i / 2) as usize).unwrap().clone());
        surrounding.push(some_platforms.get(i as usize).unwrap().clone());
        if i < size - 1 {
            surrounding.push(some_platforms.get((i + 1) as usize).unwrap().clone());
            surrounding.push(some_stamp_assemblies.get(i as usize).unwrap().clone());
        }

        let a_matched_crane_query = matched_pickdrop_query(
            some_cranes.get(i as usize).unwrap().clone(),
            some_crane_events.get(i as usize).unwrap().clone(),
            surrounding,
            &("_matched_".to_string() + &i.to_string()),
        );
        matched_crane_queries.push(a_matched_crane_query);
    }

    let mut matched_stamp_queries = vec![];
    for i in 0..(size - 1) {
        let a_matched_stamp_query = matched_stamp_query(
            some_stamp_assemblies.get(i as usize).unwrap().clone(),
            some_stamps.get(i as usize).unwrap().clone(),
            &("_matched_".to_string() + &i.to_string()),
        );
        matched_stamp_queries.push(a_matched_stamp_query);
    }

    let mut matched_conveyor_queries = vec![];
    for i in 0..(size / 2) {
        let matched_conveyor_query = matched_conveyor_query(
            some_conveyors.get(i as usize).unwrap().clone(),
            some_plastic_ramps.get(i as usize).unwrap().clone(),
            some_metal_ramps.get(i as usize).unwrap().clone(),
            some_conveyor_events.get(i as usize).unwrap().clone(),
            &("_matched_".to_string() + &i.to_string()),
        );
        matched_conveyor_queries.push(matched_conveyor_query);
    }
    let mut matched_detector_queries = vec![];
    for i in 0..size {
        let a_matched_detector_query = matched_detector_query(
            some_platforms.get(i as usize).unwrap().clone(),
            some_detectors.get(i as usize).unwrap().clone(),
            &("_matched_".to_string() + &i.to_string()));
        matched_detector_queries.push(a_matched_detector_query);
    }
    ComplexFactoryScenario {
        cranes: some_cranes,
        crane_pickdrops: some_crane_events,
        platforms: some_platforms,
        detectors: some_detectors,
        conveyors: some_conveyors,
        conveyor_events: some_conveyor_events,
        plastic_ramps: some_plastic_ramps,
        metal_ramps: some_metal_ramps,
        stamp_assemblies: some_stamp_assemblies,
        stamps: some_stamps,
        pickdrop_queries: matched_crane_queries,
        stamp_queries: matched_stamp_queries,
        detector_queries: matched_detector_queries,
        conveyor_queries: matched_conveyor_queries
    }
}
